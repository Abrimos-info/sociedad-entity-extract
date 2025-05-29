#!/usr/bin/env node
'use strict';
const JSONStream = require('JSONStream');
const es = require('event-stream');
const { Client } = require("@opensearch-project/opensearch");
const commandLineArgs = require('command-line-args');
const {JSONPath} = require('jsonpath-plus')

const optionDefinitions = [
    { name: 'uri', alias: 'u', type: String, defaultValue: 'http://localhost:9200/' },  // Opensearch URI
    { name: 'idSourceField', alias: 'i', type: String },                                // Document field to get ID from
    { name: 'idSourceDoc', alias: 'd', type: String },                                  // Entity subdocument path
    { name: 'targetIndex', alias: 't', type: String },                                  // Index that stores entities
    { name: 'targetField', alias: 'f', type: String }                                   // Index field to query with ID
];
const args = commandLineArgs(optionDefinitions);
if(!args.idSourceField || !args.targetIndex || !args.targetField ) {
    console.error('ERROR: Missing parameters.');
    process.exit(500);
}
const docQueue = {};

// Make a new Elasticsearch client
const elasticNode = args.uri;
let client = getClient(elasticNode);
process.stdin.setEncoding('utf8');

process.stdin
.pipe(JSONStream.parse())
.pipe(es.mapSync(function (obj) {
    let sourceID = getDocField(obj, args.idSourceField);

    if(sourceID.length > 0 && !docQueue[sourceID[0]]) {
        docQueue[sourceID[0]] = getDocField(obj, args.idSourceDoc)[0];
    }
}))

process.stdin.on('end', () => {
    // console.log('Processing input documents...');
    processQueue();
});

function getDocField(doc, field) {
    return JSONPath( field, doc );
}

async function processQueue() {
    let keys = Object.keys(docQueue);
    
    for(let i=0; i<keys.length; i++) {
        let id = keys[i];
        let doc = docQueue[id];
        
        // Query index for document with same ID
        let target = await findEntity(id);
        if(!target) {
            // If not found, create new document and insert into index
            let newObj = {
                nombre_razon_social: doc.name,
                nit: id,
                adjudicado: true
            }
            if(doc?.details?.legalEntityTypeDetail?.description) {
                Object.assign(newObj, { tipo_organizacion: doc.details.legalEntityTypeDetail.description });
                if(newObj.tipo_organizacion == "INDIVIDUAL")
                    Object.assign(newObj, { nombre_persona:  parseRazonSocial(newObj.nombre_razon_social)});
            }
            if(doc?.address) {
                Object.assign(newObj, {
                    departamento: doc.address.region,
                    municipio: doc.address.locality,
                    direccion: doc.address.streetAddress
                });
            }
            if(doc?.contactPoint?.telephone)
                Object.assign(newObj, { telefono: doc.contactPoint.telephone })

            process.stdout.write(JSON.stringify(newObj) + '\n');
        }
        // else console.log(JSON.stringify(target));
    }
}

function parseRazonSocial(str) {
    if(str.match(/.*,.*,.*,.*,.*/)) {
        let [ apellido1, apellido2, apellido3, nombre1, nombre2 ] = str.split(',');
        return nombre1 + (nombre2? ' ' + nombre2 : '') + ' ' + apellido1 + (apellido2? ' ' + apellido2 : '') + (apellido3? ' ' + apellido3 : '')
    }
    return null;
}

async function findEntity(id) {
    let entity = null;
    let options = {
        "index": args.targetIndex,
        "body": {
            "query": {
                "match": {
                    [args.targetField]: id
                }
            }
        }
    }
    const response = await client.search(options);

    if(response.body?.hits?.hits?.length > 0)
        entity = response.body.hits.hits[0]._source;
    
    return entity;
}

function getClient(elasticNode) {
    let client = null;
    try {
        client = new Client({ node: elasticNode, requestTimeout: 60000, maxRetries: 10, sniffOnStart: false, ssl: { rejectUnauthorized: false }, resurrectStrategy: "none", compression: "gzip" })
    }
    catch (e) {
        console.error("getClient",e);
    }
    return client;
}
