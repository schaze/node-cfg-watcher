import { ConfigFileChange, ConfigWatcher } from "./ConfigWatcher";
import { BehaviorSubject, merge, Observable, pipe, Subject } from "rxjs";
import { filter, map, mergeMap, takeUntil, tap, withLatestFrom } from "rxjs/operators";
import * as yaml from 'js-yaml';
import { Validator } from "jsonschema";
import * as winston from "winston";
import equal from 'fast-deep-equal';


export type ValidationResult<T> = {
    filename: string;
    data: T[];
    error?: Error;
} | {
    filename: string;
    data?: T[];
    error: Error;
}

export interface ConfigSet<T> {
    filename: string;
    obj: T
}

export type ConfigSetActionNames = 'add' | 'update' | 'remove' | 'moved';

export interface ConfigSetActionBase<T extends ConfigSetActionNames> {
    name: T;
}

export interface AddConfigSetAction<T> extends ConfigSetActionBase<'add'> {
    item: ConfigSet<T>,
    id: string;
}

export interface UpdateConfigSetAction<T> extends ConfigSetActionBase<'update'> {
    item: ConfigSet<T>,
    id: string;
}

export interface RemoveConfigSetAction extends ConfigSetActionBase<'remove'> {
    id: string;
}

export interface MovedConfigSetAction<T> extends ConfigSetActionBase<'moved'> {
    item: ConfigSet<T>,
    id: string;
}

export type ConfigSetAction<T> = AddConfigSetAction<T> | UpdateConfigSetAction<T> | RemoveConfigSetAction | MovedConfigSetAction<T>;


export class ConfigContentWatcher<T>{
    private _onDestroy$ = new Subject<boolean>();
    public onDestroy$ = this._onDestroy$.asObservable();

    protected validator = new Validator();

    protected _state$ = new BehaviorSubject<{ [id: string]: ConfigSet<T> }>({});
    public state$ = this._state$.asObservable();

    public get state() {
        return this._state$.value;
    }

    protected _changes$ = new Subject<ConfigSetAction<T>>();
    public changes$ = this._changes$.asObservable();

    protected readonly log: winston.Logger;

    constructor(protected watcher: ConfigWatcher, protected schema: any, protected getId: (item: T) => string) {
        this.log = winston.child({
            type: this.constructor.name
        });
    }

    public async onInit() {

        const updateConfigFile$ = this.watcher.fileChange$.pipe(
            filter(fileChange => fileChange.type === 'update' || fileChange.type === 'add'),
            this.validateConfigFile(),
            filter(validationResult => !validationResult.error),
            withLatestFrom(this.state$),
            map(([validationResult, state]) => {
                // this.log.info('add/update new config file');

                const actions = <ConfigSetAction<T>[]>[];

                const stateArray = Object.values(state);
                const stateArrayForFile = stateArray.filter(cfgSet => cfgSet.filename === validationResult.filename);


                for (let index = 0; index < stateArrayForFile.length; index++) {
                    const existingSet = stateArrayForFile[index];
                    const existingId = this.getId(existingSet.obj);

                    const newIndex = this.getIndexOfId(existingId, validationResult.data!);
                    if (newIndex === -1) { // if existing item is not found any more in the new file -> remove it
                        actions.push(<RemoveConfigSetAction>{
                            name: 'remove',
                            id: existingId
                        });
                    }
                }

                // Add new Items that do not exist yet
                for (let index = 0; index < validationResult.data!.length; index++) {
                    const newItem = validationResult.data![index];
                    const id = this.getId(newItem);

                    const existingSet = state[id];

                    if (!existingSet) { // if the object with this id does not yet exist (which should be the normal case)
                        //dispatch add action
                        actions.push(<AddConfigSetAction<T>>{
                            name: 'add',
                            id: id,
                            item: {
                                filename: validationResult.filename,
                                obj: newItem
                            }
                        });
                    } else { // if the object with this id already exists we assume it was moved between files and we received the 'add' file event before the update event from the old file
                        if (!equal(existingSet.obj, newItem)) {
                            actions.push(<UpdateConfigSetAction<T>>{
                                name: 'update',
                                id: id,
                                item: {
                                    filename: validationResult.filename,
                                    obj: newItem
                                }
                            });
                        } else {
                            if (existingSet.filename !== validationResult.filename) {
                                actions.push(<MovedConfigSetAction<T>>{
                                    name: 'moved',
                                    id: id,
                                    item: {
                                        ...existingSet,
                                        filename: validationResult.filename
                                    }
                                });
                            }
                        }

                    }
                }
                return actions;
            }),
            mergeMap(actions => actions)
        );

        const removeConfigFile$ = this.watcher.fileChange$.pipe(
            filter(fileChange => fileChange.type === 'remove'),
            withLatestFrom(this.state$),
            map(([configFile, state]) => {
                // this.log.info('remove config file');

                const actions = <ConfigSetAction<T>[]>[];

                const stateArray = Object.values(state);
                const stateArrayForFile = stateArray.filter(cfgSet => cfgSet.filename === configFile.filename);


                for (let index = 0; index < stateArrayForFile.length; index++) {
                    const existingSet = stateArrayForFile[index];
                    const existingId = this.getId(existingSet.obj);
                    actions.push(<RemoveConfigSetAction>{
                        name: 'remove',
                        id: existingId
                    });
                }
                return actions;
            }),
            mergeMap(actions => actions)
        );


        const configFileChanges$ = merge(updateConfigFile$, removeConfigFile$).pipe(
            takeUntil(this.onDestroy$),
            tap(action => {
                if (action.name === 'add' || action.name === 'update' || action.name === 'moved') {
                    // this.log.info(`[${action.item.filename}] - [${action.name}] - [${action.id}] - storeupdated`);
                    this.addOrUpdateSet(action.item);
                } else if (action.name === 'remove') {
                    this.removeSet(action.id);
                    // this.log.info(`[${action.name}] - [${action.id}] - storeupdated`);
                }

                this._changes$.next(action);
            })
        );

        configFileChanges$.subscribe();

        this.watcher.onInit();

    }

    private getIndexOfId(existingId: string, arr: T[]) {
        // Add new Items that do not exist yet
        for (let index = 0; index < arr.length; index++) {
            const item = arr[index];
            const id = this.getId(item);
            if (id === existingId) { // if id already exists
                return index;
            }
        }
        return -1;
    }

    protected addOrUpdateSet(cfgSet: ConfigSet<T>): ConfigSet<T> | null {
        if (!cfgSet) { return null; }
        const id = this.getId(cfgSet.obj);
        if (Object.prototype.hasOwnProperty.call(this.state, id)) {
            this._state$.next({ ...this.state, [id]: cfgSet })
        } else {
            this._state$.next({ ...this.state, [id]: cfgSet });
        }
        return cfgSet;
    }

    public removeSet(id: string): ConfigSet<T> | undefined {
        if (!id) { return undefined; }
        const { [id]: removedCfgSet, ...rest } = this.state;
        if (removedCfgSet) {
            this._state$.next(rest)
        }
        return removedCfgSet;
    }


    private validateConfigFile() {
        return pipe<Observable<ConfigFileChange>, Observable<ValidationResult<T>>>(
            map(fileChange => {
                try {
                    // sanitize file input
                    const data: T[] = !fileChange.content?.trim() ? [] : yaml.loadAll(fileChange.content);

                    // parse and validate input
                    const res = <ValidationResult<T>>{ filename: fileChange.filename, data: data, error: undefined }
                    for (let index = 0; index < res.data!.length; index++) {
                        const item = res.data![index];
                        const result = this.validator.validate(item, this.schema, { nestedErrors: true })
                        if (!result.valid) {
                            result.errors.forEach(error => {
                                this.log.error(`Config file ${fileChange.filename} : ${error.toString()}`);
                            })
                            throw new Error(`Config file ${fileChange.filename}: has formatting errors (see above).`);
                        }
                    }
                    return res;

                } catch (err: any) {
                    this.log.error(`Config file ${fileChange.filename} : ${err.toString()}`);
                    return <ValidationResult<T>>{ filename: fileChange.filename, data: undefined, error: err }
                }
            })
        )
    }


    public async onDestroy() {
        this._onDestroy$.next(true);
        this.watcher.onDestroy();
    }

}