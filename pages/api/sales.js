const { GoogleSpreadsheet } = require('google-spreadsheet');
const { JWT } = require('google-auth-library');
const { google } = require('googleapis');
const axios = require('axios');


export default async function sales(req, res) {

  const { query } = req;
  const address = query.address;
  const email = query.email;

  if (address && address.length == 42) {
    try {
      const sales = await processAddress(address.toLowerCase(), email, 4);
      res.status(200).json(sales);
    } catch(err) {
      console.log(err)
      res.status(500).json({});
    }
  } else {
    res.status(500).json({Error: 'Invalid address'});
  }

}


const NULL_ADDRESS = '0x0000000000000000000000000000000000000000';
const KNOWN_ADDRESSES = {
  '0x00000000006c3852cbef3e08e8df289169ede581': 'OpenSea',
  '0xf11ed77fd65840b64602526ddc38311e9923c81b': 'KnownOrigin',
  '0x5b3256965e7c3cf26e11fcaf296dfc8807c01073': 'OpenSea Wallet',
  '0x7be8076f4ea4a4ad08075c2508e481d6c946d12b': 'OpenSea Deployer 1',
  '0x7f268357a8c2552623316e2562d90e642bb538e5': 'OpenSea Wyvern Exchange v2',
  '0x0000a26b00c1f0df003000390027140000faa719': 'OpenSea Fees 3',
  '0x49128cf8abe9071ee24540a296b5ded3f9d50443': 'Foundation Deployer',
  '0xde9e5ee9e7cd43399969cfb1c0e5596778c6464f': 'KnownOrigin Fee Collector',
  '0xcda72070e455bb31c7690a170224ce43623d0b6f': 'Foundation Market',
  '0x67df244584b67e8c51b10ad610aaffa9a402fdb6': 'Foundation Treasury',
  '0x8de9c5a032463c561423387a9648c5c7bcc5bc90': 'OpenSea Fees',
  '0x53f451165ba6fdbe39a134673d13948261b2334a': 'Foundation Drop Market',
  '0xcd4ec7b66fbc029c116ba9ffb3e59351c20b5b06': 'Rarible Exchange 1',
  '0x09eab21c40743b2364b94345419138ef80f39e30': 'Rarible Exchange V1',
  '0x1cf0df2a5a20cd61d68d4489eebbf85b8d39e18a': 'Rarible Treasury',
  '0x00000000000000adc04c56bf30ac9d3c0aaf14dc': 'Seaport 1.5',
}

const FILTER_ADDRESSES = {
  '0xa9d1e08c7793af67e9d92fe308d5697fb81d3e43': 'Coinbase 10',
  '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2': 'Wrapped Ether',
  '0x7a250d5630b4cf539739df2c5dacb4c659f2488d': 'Uniswap V2: Router 2'
}

const TEXT_ARTIST = 'Artist';

const PAGE_SIZE = 1000;
const API_DELAY = 51;
const REPORT_TIME_TO_LIVE = 1 * 12 * 60 * 60 * 1000; // 12 hours
const WEI_DIVISOR = Math.pow(10, 18);


require('dotenv').config({ path: './.env' }); 

const SCOPES = [
  'https://www.googleapis.com/auth/spreadsheets',
  'https://www.googleapis.com/auth/drive.file',
];

const jwtWithImpersonation = new JWT({
  email: process.env.GOOGLE_SERVICE_CLIENT_EMAIL,
  key: process.env.GOOGLE_SERVICE_PRIVATE_KEY.split(String.raw`\n`).join('\n'),
  subject: 'support@nftydreams.com',
  scopes: SCOPES,
});

const drive = google.drive({version: 'v3', auth: jwtWithImpersonation});

function getSaleInfo() {

  return {
    utcDate: `=epochtodate(${Math.floor(Date.now() / 1000)})`,
    blockNumber: null,
    sale: 0,
    from_1: null,
    to_1: null,
    amount_1: null,
    from_2: null,
    to_2: null,
    amount_2: null,
    from_3: null,
    to_3: null,
    amount_3: null
  }
}

// skipLimit is for skipping those rows where the number of internal transactions exceeds the value
// this typically means it's not an NFT transaction, but a bulk funds transfer involving the user
async function processAddress(address, email, skipLimit) {

  let error = null;
  let docInfo = null;
  let mergedTxs = null;
  
  try {
    address = address.toLowerCase();
    let addressLookup = KNOWN_ADDRESSES;
    addressLookup[address] = TEXT_ARTIST;

    // Get all internal transactions
    // Fetch every internal transaction and then for each transaction call an end-point that
    // returns details about the transaction to build a complete picture
    const allInternalTxs = await getAllPages(fetchTx, {module: 'account', action: 'txlistinternal', field: 'address', data: address}, API_DELAY);
    const done = [];
    
    mergedTxs = [getSaleInfo()];
    mergedTxs[0].blockNumber = 'TOTAL'; // Top row has aggregate sales
    mergedTxs[0].from_1 = address;

    //allInternalTxs.length = 5;
    for(let a=0; a<allInternalTxs.length; a++) {

      if (done.indexOf(allInternalTxs[a].hash) < 0) {
        done.push(allInternalTxs[a].hash);
      } else {
        continue;
      }

      // Get the transaction summary
      const mergedTx = getSaleInfo();
      mergedTx.utcDate = `=epochtodate(${allInternalTxs[a].timeStamp})`;

      // For each internal transaction, get all the detailed transfers
      const internalTxs = await getAllPages(fetchTx, {module: 'account', action: 'txlistinternal', field: 'txhash', data: allInternalTxs[a].hash}, API_DELAY);    

      if (internalTxs.length < skipLimit) {
        // For each internal transfer do some cleanup and aggregation
        const txList = [];
        internalTxs.forEach((tx) => {
          txList.push({
            from: tx.from.toLowerCase(),
            to: tx.to.toLowerCase(),
            value: parseInt(tx.value)
          });
        });

        if (txList.length > 1 && (FILTER_ADDRESSES[txList[0].from] === undefined) && (FILTER_ADDRESSES[txList[0].to] === undefined)){ // Skip sending money
          mergedTx['sale'] = 0; // To keep a running total to get actual sale amount

          // If the internal transfer has funds going to contract and
          // then from contract to individual accounts, we don't want
          // to count that in the total so we zero it out here
          if (txList[0].to === txList[1].from) {
            txList[0].value = 0.0;
          }

          txList.forEach((t, idx) => {
            mergedTx[`from_${idx+1}`] = addressLookup[t.from] ?? t.from;
            mergedTx[`to_${idx+1}`] = addressLookup[t.to] ?? t.to;
            mergedTx[`amount_${idx+1}`] = t.value / WEI_DIVISOR;
            mergedTx['sale'] += t.value;
          });
          mergedTx['sale'] = `=hyperlink("https://etherscan.io/tx/${allInternalTxs[a].hash}", ${mergedTx['sale'] / WEI_DIVISOR})`;
          mergedTx['blockNumber'] = allInternalTxs[a].blockNumber;
    
          mergedTxs.push(mergedTx);
        }
      }
    }

    docInfo = await findOrCreateGoogleSheet(address);
    if (docInfo.updateRequired) {
      await hydrateGoogleSheet(docInfo.doc, mergedTxs, email);
    }
    console.log(`✅ Finished processing for ${address}`);
  }
  catch(e) {
    error = e.message;
  }
  return {
    address,
    email,
    docUrl: docInfo ? `https://docs.google.com/spreadsheets/d/${docInfo.doc.spreadsheetId}` : null,
    count: mergedTxs ? mergedTxs.length - 1 : null,
    error
  }
}

async function hydrateGoogleSheet(doc, sales, email) {

  // Set some styles and formatting
  // await doc.updateProperties({
  //   spreadsheetTheme: {
  //     primaryFontFamily: 'Roboto Mono',
  //     themeColors: [
  //       {colorType: 'THEME_COLOR_TYPE_UNSPECIFIED', color: },
  //       {colorType: 'TEXT', color: },
  //       {colorType: 'BACKGROUND', color: },
  //       {colorType: 'ACCENT1', color: },
  //       {colorType: 'ACCENT2', color: },
  //       {colorType: 'ACCENT3', color: },
  //       {colorType: 'ACCENT4', color: },
  //       {colorType: 'ACCENT5', color: },
  //       {colorType: 'ACCENT6', color: },
  //       {colorType: 'LINK', color: },
  //     ]
  //   }
  // });



  const sheet = doc.sheetsByIndex[0];
  await sheet.setHeaderRow(['UTC Date', 'Block', 'Sales (ETH)'
                    , 'From (1)', 'To (1)', 'Amount (1)'
                    , 'From (2)', 'To (2)', 'Amount (2)'
                    , 'From (3)', 'To (3)', 'Amount (3)']);

  await sheet.clearRows(); 
  await sheet.loadCells(`A1:O2000`);

  // HEADER ROW
  for(let t=0;t<15;t++) {
    const cell = sheet.getCell(0, t);
    cell.textFormat = { bold: true, fontSize: 12  };
    cell.backgroundColor = {
      red: 1,
      green: 0.768,
      blue: 0,
      alpha: 1
    }
  }

  // TOTAL ROW
  let totalRow = Object.values(sales[0]);
  for(let t=0;t<4;t++) {
    const cell = sheet.getCell(2, t);
    cell.textFormat = { bold: true, fontSize: 12 };
    if (t==0) {
      cell.formula = totalRow[t];
    } else if (t==2) {
      cell.formula = '=sum(C5:C2000)';
      cell.horizontalAlignment = 'RIGHT';
      cell.numberFormat = { type: 'NUMBER', pattern: '0.000000'};
    } else {
      cell.value = totalRow[t];
    }
  }

  for(let s=1;s<sales.length;s++) {
    let row = s + 4;
    let vals = Object.values(sales[s]);
    for(let v=0;v<vals.length;v++) {
      const cell = sheet.getCell(row, v);
      if (v==0) {
        cell.formula = vals[v];
      } else if (v==2) {
        cell.formula = vals[v];
        cell.numberFormat = { type: 'NUMBER', pattern: '0.000000'};
        cell.horizontalAlignment = 'RIGHT';
      } else if (v==5 || v==8 || v==11) {
        cell.numberValue = vals[v];
        cell.horizontalAlignment = 'RIGHT';
        cell.numberFormat = { type: 'NUMBER', pattern: '0.000000'};
      } else {
        cell.value = vals[v];
        if (vals[v] == TEXT_ARTIST) {
          cell.backgroundColor = {
            red: 0.87,
            green: 1,
            blue: 0,
            alpha: 1
          }
      
        }
      }
    }
  }
  await sheet.saveUpdatedCells();

  try {
    if (email) {
      await doc.share(email, {role: 'writer'});
    }  
  } catch(err) {

  }
}

async function findOrCreateGoogleSheet(address) {

    // Check if the file exists
    const res = await drive.files.list({
      q: `name contains \'${address}\' and trashed=false`,
      fields: 'nextPageToken, files(id, name, modifiedTime)',
      spaces: 'drive',
    });

    let updateRequired = false;
    let doc = null;

    if (res.data.files.length === 1) {
      doc = new GoogleSpreadsheet(res.data.files[0].id, jwtWithImpersonation);
      await doc.loadInfo();
      console.log(`Existing doc ${res.data.files[0].id} was loaded`);
      // Check if the file is more than 12 hours old
      const thresholdTime = new Date().getTime() - REPORT_TIME_TO_LIVE;
      const modifiedTime = Date.parse(res.data.files[0].modifiedTime);
      updateRequired = modifiedTime < thresholdTime;
      if (updateRequired) {
        const sheet = doc.sheetsByIndex[0];
        await sheet.clearRows();
      }
    } else {
      // Create Google Sheet
      doc = await GoogleSpreadsheet.createNewSpreadsheetDocument(jwtWithImpersonation, { title: address });
      await doc.setPublicAccessLevel('reader');
      
      // Move the file to the right folder
      const file = await drive.files.get({
        fileId: doc.spreadsheetId,
        fields: 'parents',
      });

      await drive.files.update({
        fileId: doc.spreadsheetId,
        addParents: '1rZbbPULAS1aP9Q-pB6Jq5_VWoj7dRnEG', //support@nftydreams.com/artisIQ/Lens/public
        removeParents: file.data.parents.join(','),
        fields: 'id, parents',
      });
      updateRequired = true;  

      console.log(`New doc ${doc.spreadsheetId} was created`);
    }

    return { doc, updateRequired, error: null }
}


async function getAllPages(method, options, interval) {
  let allTxs = [];
  let page = 1;   // start index is 1
  let pageTx;
  do{    
    pageTx = [];
    const result = await throttle(method, {module: options.module, action: options.action, field: options.field, data: options.data, page: page++}, interval)
    if (Array.isArray(result)) {
      pageTx = [...pageTx, ...result];
    } else {
      if (result === null) {          
      } else {
        pageTx = null;
      }
    }            

    if (pageTx === null) {
      allTxs = null;
      break;
    } else if (pageTx.length === 0) {
      break;
    } else {
      allTxs = [...allTxs,...pageTx]
    }
  } while(true && pageTx.length >= PAGE_SIZE && page < 1000)
  console.log(`${options.action}: Fetched ${allTxs ? allTxs.length : 0} transactions for ${options.data}`)

  return allTxs
}

async function throttle(method, options, interval) {
  return new Promise((resolve, reject) => {
    setTimeout(async () => {
      try {
        const result = await method.call(this, options);
        resolve(result);
      } catch(error) {
        console.log(error);
        resolve(error.message);
      }
    }, interval);
  });
}

async function fetchTx(options) { 
    const url = `https://api.etherscan.io/api?module=${options.module}&action=${options.action}` +
                `&${options.field}=${options.data}&page=${options.page}&offset=${PAGE_SIZE}&startblock=0&sort=asc` +
                `&apikey=${process.env.ETHERSCAN_API_KEY}`              
                
    const response = await axios.get(url);
    if (Array.isArray(response.data.result)) {
      return response.data.result;
    } else {
      return [response.data.result];
    }                               
}

function filterInternalTokenSalesTxs(internalTxs, tokenSalesTxs, address) {

  const salesTxs = [];
  internalTxs.forEach((t) => {
    const match = tokenSalesTxs.filter((s) => s.hash === t.hash && t.value > 0);
    if (match) {
      match.forEach((m) => {
        m.to = t.to;
        m.value = t.value;
        salesTxs.push(m);
      })
    }
  });
  // Object.values(tokenSalesTxs).forEach((tx) => {
  //   const salesTx = tx; //internalTxs.find((t) => t.hash === tx.hash);
  //   if (typeof salesTx !== 'undefined') {
  //     const txInfo= {...salesTx, ...tx}
  //     //txInfo.value = txInfo.value / Math.pow(10, 18);
  //     delete txInfo.confirmations;
  //     delete txInfo.transactionIndex;
  //     delete txInfo.tokenDecimal;
  //     delete txInfo.errCode;
  //     delete txInfo.isError;
  //     delete txInfo.nonce;
  //     delete txInfo.type;
  //     delete txInfo.input;
  //     delete txInfo.gas;
  //     delete txInfo.gasUsed;
  //     delete txInfo.gasPrice;
  //     delete txInfo.cumulativeGasUsed;
  //     delete txInfo.traceId;
  //     delete txInfo.blockHash;

  //     salesTxs.push(txInfo);
  //     console.log(txInfo)
    // } else {
    //   console.log(`${tx.hash} not found`);
    // }
 // });

  return salesTxs;
}

function filterTokenSalesTxs(txs, address) {

  // Fetch all transactions that are token mint
  const mintTxs = txs.filter((t) => t.from === NULL_ADDRESS);

  const sales = [];
  mintTxs.forEach((tx) => {
    // For each minted token, find a transaction where that token
    // is being transferred to another address
    const tokenTx = txs.filter((t) =>   t.tokenID === tx.tokenID 
                                    &&  t.contractAddress.toLowerCase() === tx.contractAddress.toLowerCase() 
                                    &&  t.from.toLowerCase() === address.toLowerCase());
    if (tokenTx) {
      tokenTx.forEach((t, index) => {
        sales.push(t);
      });
    }
  });
  return sales;
}


