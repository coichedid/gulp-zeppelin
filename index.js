'use strict';
const from = require('from2');
const toThrough = require('to-through');
const Stream = require('stream');
const Vinyl = require('vinyl');
var Client = require('node-rest-client').Client;

var client = new Client();
const readableStream = new Stream.Readable({objectMode:true});
readableStream._read = (size) => {
  if (!readableStream.alreadyGetting) readableStream.getData();
  readableStream.alreadyGetting = true;
}

var getNotebookFromZeppelin = (server, port, noteId, cb) => {
  const url = `http://${server}:${port}/api/notebook/export/${noteId}`
  client.get(url, (data, response) => {
    // const message = data.message.replace(new RegExp('\n','g'),'');
    // console.log(data.message);
    const obj = JSON.parse(data.body);
    var allTextContent = "";
    obj.paragraphs.forEach((p) => {
      allTextContent += `\n${p.text}`;
    });

    var allTextArray = allTextContent.split('\n');
    var allTextArrayFiltered = [];
    const regex1 = /%pyspark/;
    const regex2 = /## (args = getResolvedOptions\(sys\.argv, \[.*\]\))/;
    const regex3 = /args = {'JOB_NAME': '.*', 'bucket': '.*', 'key': '.*', 'origin': '.*'}/;
    const regex4 = /.*\.printSchema\(\)/;
    const regex5 = /.*\.show\(.*\)/;
    // const regex6 = /(bucket|key|origin) = ".*"/; Used default

    allTextArray.forEach( (l) => {
      var include = true;
      if (regex1.test(l)) include = false;
      else if (regex2.test(l)) {
        include = true;
        var parsed = regex2.exec(l);
        l = parsed[1];
      }
      else if (regex3.test(l)) include = false;
      else if (regex4.test(l)) include = false;
      else if (regex5.test(l)) include = false;
      // else if (regex6.test(l)) include = false; used default

      if(include) allTextArrayFiltered.push(l);
    });
    allTextContent = allTextArrayFiltered.join('\n');
    // Clear %pyspark
    // var pysparkRegexp = new RegExp('%pyspark\n','g');
    // allTextContent = allTextContent.replace(pysparkRegexp,'');
    const result = {
      allTextContent:allTextContent,
      notebook:obj,
      name: obj.name
    }
    return cb(null, result);
  });
};

class ZeppelinClient {
  constructor(zeppelinServer, zeppelinPort) {
    this.server = zeppelinServer;
    this.port = zeppelinPort;
  }

  getNotebook(noteId, filename) {
    readableStream.getData = () => {

      getNotebookFromZeppelin(this.server, this.port, noteId, (err, data) => {
        if(err) {
          console.log(err);
          return readableStream.push(null);
        }
        var fileAllTextContent = new Vinyl({
          path:`./${filename}_script.exec`,
          contents:Buffer.from(data.allTextContent)
        });
        var str = JSON.stringify(data.notebook);
        var fileNotebook = new Vinyl({
          path:`./${filename}_notebook.json`,
          contents:Buffer.from(str)
        });
        readableStream.push(fileAllTextContent);
        readableStream.push(fileNotebook);
        readableStream.push(null)
      });
    }
    return toThrough(readableStream);
  }
}

module.exports = ZeppelinClient;
