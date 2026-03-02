const fs = require('fs');
const path = require('path');

const { chain } = require('stream-chain');
const { parser } = require('stream-json');
const { pick } = require('stream-json/filters/Pick');
const { streamArray } = require('stream-json/streamers/StreamArray');

// INPUT FILE (this is your renamed zoning atlas)
const inputFile = path.join(__dirname, '../publicData/zoning/ZoningBoundaryData.geojson');

// OUTPUT FOLDER (this is where we'll dump the split files)
const outputFolder = path.join(__dirname, '../publicData/zoning/split');

if (!fs.existsSync(outputFolder)) {
  fs.mkdirSync(outputFolder, { recursive: true });
}

console.log('Streaming zoning boundary file:', inputFile);

// Build the streaming pipeline
const pipeline = chain([
  fs.createReadStream(inputFile),
  parser(),
  pick({ filter: 'features' }),
  streamArray()
]);

let count = 0;

pipeline.on('data', ({ value }) => {
  const town = value?.properties?.TOWN || `unknown_${count}`;
  const filename = path.join(outputFolder, `${town.replace(/\s+/g, '_')}.json`);

  fs.writeFileSync(filename, JSON.stringify(value));

  count++;
});

pipeline.on('end', () => {
  console.log('Finished splitting zoning boundaries.');
  console.log(`Total features processed: ${count}`);
});
