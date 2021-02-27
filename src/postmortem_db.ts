const mongodb = require("mongodb");
const processLib = require("process");
const fs = require("fs");
const path = require("path");
const BSON = require('bson');
import * as assert from "assert";
import { db_spawner } from "./db_spawner";
import { MongoClient, Db } from "mongodb";
import { decodedTextSpanIntersectsWith } from "typescript";

namespace postmortem_db {
  export class PostmortemDb {
    uri: string;
    incident: string;
    path: string;
    db: MongoClient;
    incidentDb: Db;
    filter: string;

    constructor(uri: string, incident: string) {
      this.uri = uri;
      this.incident = incident;
      this.path = "";
    }

    setPath(path: string) {
      this.path = path;
    }
    setFilter(filter: string) {
      this.filter = filter;
    }

    async init() {
      console.log("Starting...");
      this.db = await MongoClient.connect(this.uri, {
        useUnifiedTopology: true,
      });
      var metadata = await this.readOrCreateIncident();
      console.log("Records: " + JSON.stringify(metadata));

      var dataLoadResult = await this.loadDataIfNeeded();
      console.log("Data load: " + JSON.stringify(dataLoadResult));
    }

    async readOrCreateIncident(): Promise<JSON> {
      console.log("Check incident " + this.incident);
      this.incidentDb = this.db.db(this.incident);
      const coll = this.incidentDb.collection("meta");
      var result = await coll.findOne({ incident: this.incident });

      if (result) {
        this.path = result.path;
        console.log("Incident path " + this.path);
        return result;
      }
      if (!this.path)
        throw "--path is required to init the Metadata for the first time";
      console.log("Create new Meta record");
      var insertResult = await coll.insertOne({
        incident: this.incident,
        path: this.path,
      });
      var meta = await coll.findOne({ incident: this.incident });
      if (meta == null) throw 'Write failed';
      return meta;
    }

    async loadDataIfNeeded(): Promise<JSON> {
      console.log("Check if data needs to be loaded...");
      var dataPath = path.join(this.path, "data/db/job0/resmoke/");
      for (const replicaSet of fs.readdirSync(dataPath)) {
          for (const node of fs.readdirSync(path.join(dataPath, replicaSet))) {
            let nodePath: string = path.join(dataPath, replicaSet, node);
            if (this.filter && !nodePath.match(this.filter)) {
              console.log('Skipping node ' + nodePath + ' as not matching the filter');
              continue;
            }
            console.log(
              "Checking node " + node + " for replica set " + replicaSet
            );
            let result = await this.loadDataForNodeIfNeeded(
              replicaSet,
              node,
              nodePath
            );
            console.log(result);
        }
      }
      return <JSON>{};
    }

    async loadDataForNodeIfNeeded(
      replicaSet: string,
      node: string,
      path: string
    ): Promise<JSON> {
      try {
        const coll = this.incidentDb.collection("meta");
        var docCursor = await coll.find({
          replicaSet: replicaSet,
          node: node,
        });
        if ((await docCursor.count()) !== 0) {
          var docs = docCursor.toArray();
          console.log('Already loaded ' + JSON.stringify(docs));
          // return JSON.stringify(docs[0]);
          return <JSON>{};
        }
        var loadReasult = await this.loadDataForNode(replicaSet, node, path);
      } finally {
        return <JSON>{};
      }
    }

    async loadDataForNode(
      replicaSet: string,
      node: string,
      path: string
    ): Promise<JSON> {
      var spawner = new db_spawner.DbSpawner(path, replicaSet + "_" + node);
      await spawner.spawn();
      await spawner.connect();
      var dbs: string[] = await spawner.listDbs();
      for (const db of dbs) {
        var colls = await spawner.listCollections(db);
        for (const coll of colls) {
          var targetCollection = this.incidentDb.collection(coll);
          await (await spawner.loadData(db, coll)).forEach(
            function(doc: any) {
              doc['id'] = doc['_id'];
              doc['_id'] = doc['_id'] + '_' + replicaSet + '_' + node;
              doc['replicaSet'] = replicaSet;
              doc['node'] = node;
              console.log(doc);

            }
          );
        }
      }

      await this.loadOplogForNode(replicaSet, node, spawner, 'oplog');
      var shutdownRes = await spawner.shutdown();
      console.log('Shutdown: ' + JSON.stringify(shutdownRes));
      return <JSON>{};
    }
  
    async loadOplogForNode(
      replicaSet: string,
      node: string,
      spawner: db_spawner.DbSpawner,
      targetCollection: string
    ): Promise<JSON> {
      console.log('load oplog...');
      let count: number = 0;
      let self: PostmortemDb = this;
      let oplogData: Array<JSON> = [];
      await (await spawner.loadData('local', 'oplog.rs')).forEach(
        function(doc: any) {
            count++;
            doc['_id'] = replicaSet + '_' + node + '_' + String(count);
            doc['replicaSet'] = replicaSet;
            doc['node'] = node;
            doc['count'] = count;
            if (doc['ui']) {
              doc['ui_data'] = self.decodeId(doc['ui']['buffer']);
            }
            // Fix the $ prefix
            if (doc['o'] && doc['o']['$v']) {
              doc['o']['_$v'] = doc['o']['$v'];
              delete doc['o']['$v'];
            }
            //console.log(doc);
            oplogData.push(doc);
          });
      console.log(`loaded ${oplogData.length} oplog documents`);

      for (const doc of oplogData) {
        try {
          let res = await self.incidentDb.collection("oplog").update(
            { _id: doc['_id'] },
            doc,
            { upsert: true }
          );
          //console.log(res.result);
        } catch(err) {
          console.log(`Failed: ${err} in `, doc);
        }
      }

      return <JSON>{};
    }

    decodeId(id: string) {
      return Buffer.from(id, 'base64').toString('binary');
    }

    // Helper to execute an array of async callbacks.
    async asyncForEach(array: any, callback: any) {
      for (let index = 0; index < array.length; index++) {
        await callback(array[index], index, array);
      }
    }
  }
} // namespace

export { postmortem_db };
