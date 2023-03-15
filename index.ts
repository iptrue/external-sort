import { createReadStream, createWriteStream } from "fs";
import { rm } from "fs/promises";
import { pipeline } from "stream/promises";
import readline from "readline";
import buffer from "buffer";

const BUFFER_CAPACITY = 100_000_000;
const MAX_MEM_USE = 100_000_000;
const FILE_SIZE = 1_000_000_000;

(async function () {
  const fileName = "randomStrings-chaos.txt";
  await createLargeFile(fileName);
  await externSort(fileName);
})();

async function externSort(fileName: string) {
  const file = createReadStream(fileName, {
    highWaterMark: BUFFER_CAPACITY / 10,
  });
  const lines = readline.createInterface({ input: file, crlfDelay: Infinity });
  const v: string[] = [];
  let size = 0;
  const tmpFileNames: string[] = [];
  for await (let line of lines) {
    size += line.length;
    v.push(line);
    if (size > MAX_MEM_USE / 10) {
      await sortAndWriteToFile(v, tmpFileNames);
      size = 0;
    }
  }
  if (v.length > 0) {
    await sortAndWriteToFile(v, tmpFileNames);
  }
  await merge(tmpFileNames, fileName);
  await cleanUp(tmpFileNames);
}

function cleanUp(tmpFileNames: string[]) {
  return Promise.all(tmpFileNames.map((f) => rm(f)));
}

async function merge(tmpFileNames: string[], fileName: string) {
  console.log("merging result ...");
  const resultFileName = `${fileName.split(".txt")[0]}-sorted.txt`;
  const file = createWriteStream(resultFileName, {
    highWaterMark: BUFFER_CAPACITY / 10,
  });
  const activeReaders = tmpFileNames.map((name) =>
    readline
      .createInterface({
        input: createReadStream(name, { highWaterMark: BUFFER_CAPACITY / 10 }),
        crlfDelay: Infinity,
      })
      [Symbol.asyncIterator]()
  );
  const values = await Promise.all<string>(
    activeReaders.map((r) => r.next().then((e) => e.value))
  );
  return pipeline(async function* () {
    const reducer = (prev: any, cur: any, idx: number) =>
      cur < prev[0] ? [cur, idx] : prev;
    while (activeReaders.length > 0) {
      const [minVal, i] = values.reduce(reducer, [
        "~".repeat(buffer.constants.MAX_STRING_LENGTH / 2 - 1),
        0,
      ]);

      yield `${minVal}\n`;
      const res = await activeReaders[i].next();
      if (!res.done) {
        values[i] = res.value;
      } else {
        values.splice(i, 1);
        activeReaders.splice(i, 1);
      }
    }
  }, file);
}

async function sortAndWriteToFile(v: string[], tmpFileNames: string[]) {
  v.sort();
  let tmpFileName = `tmp_sort_${tmpFileNames.length}.txt`;
  tmpFileNames.push(tmpFileName);
  console.log(`creating tmp file: ${tmpFileName}`);
  await pipeline(
    v.map((e) => `${e}\n`),
    createWriteStream(tmpFileName, { highWaterMark: BUFFER_CAPACITY / 10 })
  );
  v.length = 0;
}

function createLargeFile(fileName: string) {
  console.log("Creating large file ...");
  return pipeline(
    randomStrings(),
    createWriteStream(fileName, { highWaterMark: BUFFER_CAPACITY / 10 })
  );
}

function makeid(length: number): string {
  let result = "";
  const characters =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
  const charactersLength = characters.length;
  let counter = 0;
  while (counter < length) {
    result += characters.charAt(Math.floor(Math.random() * charactersLength));
    counter += 1;
  }
  return result;
}

function* randomStrings(): Generator<string> {
  let readBytes = 0;
  let lastLog = 0;
  while (readBytes < FILE_SIZE) {
    let x: string = makeid(7);
    const data = `${x}\n`;
    readBytes += data.length;
    if (readBytes - lastLog > 1_000_000) {
      console.log(`${readBytes / 1_000_000.0}mb`);
      lastLog = readBytes;
    }
    yield data;
  }
}
