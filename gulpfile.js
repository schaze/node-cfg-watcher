const gulp = require('gulp');
const ts = require('gulp-typescript');
const sourcemaps = require('gulp-sourcemaps');
const jeditor = require("gulp-json-editor");

const spawn = require('child_process').spawn;
const ASSETS = ['src/**/*.json', 'src/**/*.json', 'README.md', 'LICENSE'];
const PACKAGE_JSON = 'package.json';
const DEST = !!process.env['LIB_DEST'] ? process.env['LIB_DEST'] : 'lib';
const merge = require('merge2')

var server = null;

// pull in the project TypeScript config
const tsProject = ts.createProject('./tsconfig.json');

function devBuild() {
  const tsResult = tsProject.src()
    .pipe(sourcemaps.init({ debug: true }))
    .pipe(tsProject());
  return merge([
    tsResult.dts.pipe(gulp.dest(DEST)),
    tsResult.js
      .pipe(sourcemaps.mapSources(function (sourcePath, file) {
        // source paths are prefixed with '../src/'
        return sourcePath.slice(1);
      }))
      .pipe(sourcemaps.write('.', { includeContent: false, sourceRoot: function (file) { return file.cwd + '/src/'; } }))
      .pipe(gulp.dest(DEST))
  ]);
}


function prodBuild() {
  const tsResult = tsProject.src()
    .pipe(sourcemaps.init())
    .pipe(tsProject());

  return merge([
    tsResult.dts.pipe(gulp.dest(DEST)),
    tsResult.js.pipe(gulp.dest(DEST))
  ]);

}



function assets() {
  return gulp.src(ASSETS).pipe(gulp.dest(DEST));
}

function updatePackageJSON() {
  return gulp.src([PACKAGE_JSON])
    .pipe(jeditor(
      (json) => {
        json.devDependencies = {};
        delete json.scripts.test;
        delete json.scripts.gulp;
        // json.main = json.name + '.js';
        return json;
      }
    ))
    .pipe(gulp.dest(DEST));
}



function watch(done) {
  gulp.watch(['./src/**/*.ts', './src/**/*.json'], gulp.series(gulp.parallel(devBuild, assets)));
  done();
}

function watchTest(done) {
  gulp.watch(['./src/**/*.ts', './src/**/*.json'], gulp.series(gulp.parallel(testBuild, assets)));
  done();
}


gulp.task('default', gulp.series(
  // 'upgradePKG', 
  devBuild,
  assets));

gulp.task('prod', gulp.series(prodBuild, assets, updatePackageJSON));
gulp.task('watch', gulp.series(devBuild, assets, watch));
