// Run this script as:
//   npm run postmortem -- --url localhost:27017

const chalk = require("chalk");
const processLib = require("process");
import { postmortem_db } from "./postmortem_db";

var argv = require("minimist")(process.argv.slice(2));

function help() {
  console.log(`Help:
      --url: MongoDB connection
      --incident: ID of the incident to use
      --path: path to incident data (uncompressed), should contain 'data' and 'logs' dirs
      --filter: node subpath regex to load, instead of circling all paths
      `);
}

if (!argv.url || !argv.incident || argv.help) {
  help();
  processLib.exit(1);
}

let db = new postmortem_db.PostmortemDb(argv["url"], argv["incident"]);
if (argv["path"]) {
  db.setPath(argv["path"]);
}
if (argv.filter) {
  db.setFilter(argv.filter);
}

db.init()
  .then((text) => {
    console.log(text);
    //processLib.exit(0);
  })
  .catch((err) => {
    console.log('Error: ', err);
    processLib.exit(1);
  });
