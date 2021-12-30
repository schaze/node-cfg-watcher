import { connectable, ConnectableObservable, merge, of, Subject, Subscription } from "rxjs";
import * as k8s from '@kubernetes/client-node';
import { Config, V1ConfigMap } from "@kubernetes/client-node";
import * as winston from "winston";
import { readFileRx, watchRx } from "watch-rx-ts";
import { join } from "path";
import { existsSync } from "fs";
import { delay, filter, map, mergeMap, retry,  switchMap, takeUntil, tap } from "rxjs/operators";
import { watchKubeResourceRx } from "k8s-watch-rx";

function inputIsNotNullOrUndefined<T>(input: null | undefined | T): input is T {
    return input !== null && input !== undefined;
}


let instanceID = 0;

export interface ConfigFileChange {
    type: 'add' | 'update' | 'remove';
    filename: string;
    content: string | null;
}

export interface ConfigFile {
    filename: string;
    content: string;
}


export abstract class ConfigWatcher {
    protected onDestroy$ = new Subject<boolean>();

    protected _fileStore = new Map<string, string>();
    public get fileStore(): ReadonlyMap<string, string>{
        return this._fileStore;
    }

    protected _fileChange$ = new Subject<ConfigFileChange>();
    public fileChange$ = this._fileChange$.asObservable();

    protected readonly log: winston.Logger;

    constructor() {
        this.log = winston.child({
            type: this.constructor.name,
            name: `cw-${instanceID++}`
        });
    }

    abstract onInit(): Promise<void>

    async onDestroy(): Promise<void> {
        this.onDestroy$.next(true);
    }

}

export class ConfigMapWatcher extends ConfigWatcher {
    private connectableSubscription?: Subscription;

    constructor(private configMapName: string) {
        super();
    }

    async onInit(): Promise<void> {
        const config = new k8s.KubeConfig();
        config.loadFromDefault();
        const context = config.getContextObject(config.currentContext);

        const cfgMapWatchPath$ = of(context).pipe(
            switchMap(context => {
                if (context?.namespace) {
                    return of(context.namespace);
                } else {
                    const nsPath = join(Config.SERVICEACCOUNT_ROOT, 'namespace');
                    if (existsSync(nsPath)) {
                        return readFileRx(nsPath, 'utf-8').pipe(map(ns => ns?.trim()));
                    } else {
                        return of('default')
                    }
                }
            }),
            map(namespace => `/api/v1/watch/namespaces/${namespace}/configmaps/${this.configMapName}`)
        );

        const watcher$ =
            connectable(
                cfgMapWatchPath$.pipe(
                    switchMap(cfgMapWatchPath => watchKubeResourceRx<V1ConfigMap>(cfgMapWatchPath).pipe(retry()))
                ),
                { connector: () => new Subject()}
            );

        const addedModifiedConfigMap$ = watcher$.pipe(
            filter(event => event.phase === 'ADDED' || event.phase === 'MODIFIED'),
            map(event => {
                const fileChanges: ConfigFileChange[] = [];
                const data = event.apiObj?.data;

                if (data && Object.keys(data).length > 0) {
                    for (const filename in data) {
                        if (Object.prototype.hasOwnProperty.call(data, filename)) {
                            const content = data[filename];

                            if (!this._fileStore.has(filename)) {
                                fileChanges.push({ type: 'add', filename, content });
                                this._fileStore.set(filename, content);
                            } else {
                                const oldContent = this._fileStore.get(filename);
                                if (oldContent !== content) {
                                    fileChanges.push({ type: 'update', filename, content });
                                    this._fileStore.set(filename, content);
                                }
                            }

                        }
                    }
                    for (const [filename, _] of this._fileStore) {
                        if (!Object.prototype.hasOwnProperty.call(data, filename)) {
                            fileChanges.push({ type: 'remove', filename, content: null });
                            this._fileStore.delete(filename);
                        }
                    }
                }

                const binaryData = event.apiObj?.binaryData;

                if (binaryData && Object.keys(binaryData).length > 0) {
                    for (const filename in binaryData) {
                        if (Object.prototype.hasOwnProperty.call(binaryData, filename)) {
                            const content = String(Buffer.from(binaryData[filename], 'base64').toString('utf8'));

                            // console.log(content);
                            if (!this._fileStore.has(filename)) {
                                fileChanges.push({ type: 'add', filename, content });
                                this._fileStore.set(filename, content);
                            } else {
                                const oldContent = this._fileStore.get(filename);
                                if (oldContent !== content) {
                                    fileChanges.push({ type: 'update', filename, content });
                                    this._fileStore.set(filename, content);
                                }
                            }

                        }
                    }
                    for (const [filename, _] of this._fileStore) {
                        if (!Object.prototype.hasOwnProperty.call(binaryData, filename)) {
                            fileChanges.push({ type: 'remove', filename, content: null });
                            this._fileStore.delete(filename);
                        }
                    }

                }

                return fileChanges;

            }),
            mergeMap(changes => changes)

        );

        const removedConfigMap$ = watcher$.pipe(
            filter(event => event.phase === 'DELETED'),
            map(event => {
                const fileChanges: ConfigFileChange[] = [];
                for (const [filename, _] of this._fileStore) {
                    fileChanges.push({ type: 'remove', filename, content: null });
                }
                this._fileStore.clear();
                return fileChanges;
            }),
            mergeMap(changes => changes)
        );

        merge(addedModifiedConfigMap$, removedConfigMap$).pipe(
            takeUntil(this.onDestroy$),
            filter(inputIsNotNullOrUndefined),
            tap(change => {
                this._fileChange$.next(change);
            })
        ).subscribe({
            error: (err) => { this.log.error(`Error watching for configmap ${this.configMapName}`, { err }) }
        });

        this.connectableSubscription = watcher$.connect();
    }

    public override async onDestroy() {
        super.onDestroy();
        this.connectableSubscription?.unsubscribe();
    }

}

export class ConfigFileWatcher extends ConfigWatcher {
    private connectableSubscription?: Subscription;

    constructor(private paths: string[] = []) {
        super();
    }

    async onInit(): Promise<void> {

        const watcher$ = new ConnectableObservable(
            watchRx(this.paths, { followSymlinks: true }),
            () => new Subject()
        );

        // const watcher$ = watchRx(this.paths, { followSymlinks: true });

        const addedModifiedConfigFile$ = watcher$.pipe(
            filter(event => event.event === 'add' || event.event === 'change'),
            delay(500),
            mergeMap(file => readFileRx(file.fullname, 'utf8').pipe(
                map(content => {
                    return <ConfigFile>{ filename: file.basename, content }
                }))),
            map(cfgFile => {
                if (!this._fileStore.has(cfgFile.filename)) {
                    this._fileStore.set(cfgFile.filename, cfgFile.content);
                    return <ConfigFileChange>{ type: 'add', ...cfgFile }
                } else {
                    const oldContent = this._fileStore.get(cfgFile.filename);
                    if (oldContent !== cfgFile.content) {
                        this._fileStore.set(cfgFile.filename, cfgFile.content);
                        return <ConfigFileChange>{ type: 'update', ...cfgFile };
                    }
                }
                return null;
            })
        );

        const removedConfigFile$ = watcher$.pipe(
            filter(event => event.event === 'unlink'),
            delay(500),
            map(event => {
                if (this._fileStore.has(event.basename)) {
                    this._fileStore.delete(event.basename);
                    return <ConfigFileChange>{ type: 'remove', filename: event.basename, content: null }
                }
                return null;
            }),
        );

        merge(addedModifiedConfigFile$, removedConfigFile$).pipe(
            takeUntil(this.onDestroy$),
            filter(inputIsNotNullOrUndefined),
            tap(change => {
                this._fileChange$.next(change)
                this.log.info(`${change.type} - [${change.filename}]`);
            })
        ).subscribe({
            error: (err) => { this.log.error(`Error watching for filechages on: `, { paths: this.paths, err }) }
        });


        this.connectableSubscription = watcher$.connect();
    }

    public override async onDestroy() {
        super.onDestroy();
        this.connectableSubscription?.unsubscribe();
    }
}