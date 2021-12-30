# cfg-watcher

## About
Simple config watcher. 
Can watch changes on local files or kubernetes configmaps, handles input validation via JSON Schema and emits changes file contents.

## Basic usage example:

```typescript

import { join } from "path";
import { ConfigContentWatcher, ConfigFileWatcher, ConfigMapWatcher } from "cfg-watcher";

const cfgSchema = require?.main?.require('./cfgItems.JSONSchema.json');

const CFG_FOLDER='./cfg';
const CFG_CFGMAP_NAME='app-config';

const BACKEND = 'kubernetes';

const watcher = (BACKEND === 'file' ?
            new ConfigFileWatcher(['*.yaml', '*.yml'].map(fileFilter => join(CFG_FOLDER, fileFilter))) :
            new ConfigMapWatcher(CFG_CFGMAP_NAME));

const contentWatcher = new ConfigContentWatcher(
    watcher, 
    cfgSchema, 
    cfgItem => cfgItem.id); // provide a method to obtain cfg item id



contentWatcher.changes$.subscribe({
    next: change => {
        if (change.name === 'remove') {
            console.log('Removed cfg item: ', change.id);
        } else if (change.name === 'add') {
            console.log('Added cfg item: ', change.item.obj);
        } else if (change.name === 'update') {
            console.log('Updated cfg item: ',change.item.obj);
        }
    }
});

contentWatcher.onInit();


// on application close
contentWatcher.onDestroy();

```