'use strict';
const from = require('from2');
const toThrough = require('to-through');
const Stream = require('stream');
const Vinyl = require('vinyl');
var Client = require('node-rest-client').Client;
const readableStream = new Stream.Readable({objectMode:true});
readableStream._read = (size) => {
  if (!readableStream.alreadyGetting) readableStream.getData();
  readableStream.alreadyGetting = true;
}

var getNotebookStruct = (paragraphsObj, paragraphTypes) => {
  var struct = {};
  paragraphsObj.paragraphs.forEach((p) => {
    const key = p.title;
    if (key.lenght > 0) {
      if (paragraphTypes.includes(key))
        struct[key] = p.text;
    }
  });
  return struct;
}

var getNotebookFromZeppelin = (server, port, noteId, proxyOptions, paragraphTypes, cb) => {
  var client = new Client(proxyOptions);
  const url = `http://${server}:${port}/api/notebook/export/${noteId}`
  client.get(url, (data, response) => {
    // const message = data.message.replace(new RegExp('\n','g'),'');
    // console.log(data.message);
    const obj = JSON.parse(data.body);
    var struct = getNotebookStruct(obj, paragraphTypes);

    const regex1 = /%pyspark/;
    // const regex2 = /## (args = getResolvedOptions\(sys\.argv, \[.*\]\))/;
    // const regex3 = /args = {'JOB_NAME': '.*', 'sqs_url': '.*'}/;
    const regex4 = /.*\.printSchema\(\)/;
    const regex5 = /.*\.show\(.*\)/;
    // const regex6 = /(bucket|key|origin) = ".*"/; Used default

    paragraphTypes.forEach((k) => {
      var allTextContent = struct[k];
      var allTextArray = allTextContent.split('\n');
      var allTextArrayFiltered = [];

      allTextArray.forEach( (l) => {
        var include = true;
        if (regex1.test(l)) include = false;
        // else if (regex2.test(l)) {
        //   include = true;
        //   var parsed = regex2.exec(l);
        //   l = parsed[1];
        // }
        // else if (regex3.test(l)) include = false;
        else if (regex4.test(l)) include = false;
        else if (regex5.test(l)) include = false;
        // else if (regex6.test(l)) include = false; used default

        if(include) allTextArrayFiltered.push(l);
      });

      struct[k] = allTextArrayFiltered.join('\n');
    })
    // var allTextContent = "";
    // obj.paragraphs.forEach((p) => {
    //   allTextContent += `\n${p.text}`;
    // });

    // var allTextArray = allTextContent.split('\n');
    // var allTextArrayFiltered = [];
    // const regex1 = /%pyspark/;
    // const regex2 = /## (args = getResolvedOptions\(sys\.argv, \[.*\]\))/;
    // const regex3 = /args = {'JOB_NAME': '.*', 'sqs_url': '.*'}/;
    // const regex4 = /.*\.printSchema\(\)/;
    // const regex5 = /.*\.show\(.*\)/;
    // // const regex6 = /(bucket|key|origin) = ".*"/; Used default
    //
    // allTextArray.forEach( (l) => {
    //   var include = true;
    //   if (regex1.test(l)) include = false;
    //   else if (regex2.test(l)) {
    //     include = true;
    //     var parsed = regex2.exec(l);
    //     l = parsed[1];
    //   }
    //   else if (regex3.test(l)) include = false;
    //   else if (regex4.test(l)) include = false;
    //   else if (regex5.test(l)) include = false;
    //   // else if (regex6.test(l)) include = false; used default
    //
    //   if(include) allTextArrayFiltered.push(l);
    // });
    // allTextContent = allTextArrayFiltered.join('\n');
    // // Clear %pyspark
    // // var pysparkRegexp = new RegExp('%pyspark\n','g');
    // // allTextContent = allTextContent.replace(pysparkRegexp,'');
    const result = {
      // allTextContent:allTextContent,
      struct: struct,
      notebook:obj,
      name: obj.name
    }
    return cb(null, result);
  });
};

var genFiles = (struct, fileStructures) => {
  fileStructures.forEach((fileStructure) => {
    const fname = fileStructure.filename;
    const keys = fileStructure.paragraphs;
    const extension = fileStructure.extension;
    const basePath = fileStructure.basePath;
    var text = "";
    keys.forEach((k) => {
      t = k.type;
      v = k.value;
      if (t == 'key') text += `\n${struct[k]}`;
      else if (t == 'text') text += `\n${v}`;
    })
    var file = new Vinyl({
      path:`./${basePath}/${fname}.${extension}`,
      contents:Buffer.from(text)
    });
    readableStream.push(file);
  });
}

class ZeppelinClient {
  constructor(zeppelinServer, zeppelinPort, useProxy, proxyHost, proxyPort) {
    this.server = zeppelinServer;
    this.port = zeppelinPort;
    if (useProxy) {
      this.proxyOptions = {
          proxy: {
              host: proxyHost,
              port: proxyPort,
              tunnel: true
          }
      }
    }
  }

  getNotebook(noteId, filename, paragraphTypes, fileStructures) {
    readableStream.getData = () => {

      getNotebookFromZeppelin(this.server, this.port, noteId, this.proxyOptions, paragraphTypes, (err, data) => {
        if(err) {
          console.log(err);
          return readableStream.push(null);
        }
        const struct = data.struct;
        genFiles(struct, fileStructures);
        // var fileAllTextContent = new Vinyl({
        //   path:`./${filename}_script.exec`,
        //   contents:Buffer.from(data.allTextContent)
        // });
        var str = JSON.stringify(data.notebook);
        var fileNotebook = new Vinyl({
          path:`./${filename}_notebook.json`,
          contents:Buffer.from(str)
        });
        // readableStream.push(fileAllTextContent);
        readableStream.push(fileNotebook);
        readableStream.push(null)
      });
    }
    return toThrough(readableStream);
  }
}

module.exports = ZeppelinClient;
