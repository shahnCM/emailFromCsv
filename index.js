const fs = require('fs')
const csv = require('csv-parser')

// Example usage: Log memory usage every second
const memoryUsageInterval = setInterval(logMemoryUsage, 500);
// Stop logging memory usage after 5 seconds (for demonstration purposes)
setTimeout(() => clearInterval(memoryUsageInterval), 6000);

const FILE_PATH = './emails_list.csv'
const CHUNK_SIZE = process.env.CHUNK_SIZE_IN_MB * 1024 * 1024 // 50 MB

readLargeCsvFile().catch((err) => console.error(err))

async function readLargeCsvFile() {
  const fileStats = fs.statSync(FILE_PATH)
  const fileSize = fileStats.size
  console.log('File Size: ', fileSize / (1024 * 1024), ' MB')
  const readStream = fs.createReadStream(FILE_PATH)
  let currentChunk = []
  let currentChunkSize = 0
  let totalEmails = 0

  readStream
    .pipe(csv({ delimiter: '\n' }))
    .on('data', (data) => {
      currentChunk.push(data)
      currentChunkSize += JSON.stringify(data).length
      if (currentChunkSize >= CHUNK_SIZE) {
        readStream.pause()
        let chunkData = currentChunk
        currentChunk = []
        currentChunkSize = 0
        readStream._readableState.buffer.clear()
        readStream._readableState.length = 0
        processChunk(chunkData)
          .then(total => {
            totalEmails += total
            readStream.resume()
          })
      }
    })
    .on('end', () => {
      if (currentChunk.length > 0) {
        processChunk(currentChunk)
          .then(total => {
            totalEmails += total
            currentChunk = []
            currentChunkSize = 0
            console.log('CSV reading completed.')
            console.log('Total Emails in CSV: ', totalEmails)
          })
      }
    })
    .on('error', (e) => {
      throw new Error(e)
    })
}

function processChunk(chunk) {
    try {
      let total = 0
      chunk.forEach(row => {
        total++
        sendEmail({ email: row.EMAILS})
      })
      console.log('Total Emails in Chunk: ', total)
      return Promise.resolve(total)
    } catch (e) {
      return Promise.reject('ERROR: ', e)
    }
}

function sendEmail(data, tries = 1, error = null) {
  if(tries > 2) {
    return Promise.reject(error)
  } 
  try {
    // use Nodemailer or any other transport
    // console.log('Mail to: ', data.email)
    return Promise.resolve(true)
  } catch (e) {
    return sendEmail(data, ++tries, e)
  }
}

function logMemoryUsage() {
  const memoryUsage = process.memoryUsage();
  console.log('Memory Usage:');
  console.log(`- RSS: ${formatBytes(memoryUsage.rss)}`);
  console.log(`- Heap Total: ${formatBytes(memoryUsage.heapTotal)}`);
  console.log(`- Heap Used: ${formatBytes(memoryUsage.heapUsed)}`);
  console.log(`- External: ${formatBytes(memoryUsage.external)}`);
  console.log(`- Array Buffers: ${formatBytes(memoryUsage.arrayBuffers)}`);
}

function formatBytes(bytes) {
  const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
  if (bytes === 0) return '0 Bytes';
  const i = Math.floor(Math.log(bytes) / Math.log(1024));
  return parseFloat((bytes / Math.pow(1024, i)).toFixed(2)) + ' ' + sizes[i];
}
