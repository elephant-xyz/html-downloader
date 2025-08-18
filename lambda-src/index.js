const AWS = require("aws-sdk");
const S3 = new AWS.S3();
const chromium = require("@sparticuz/chromium");
const puppeteer = require("puppeteer-core");
const { parse } = require("csv-parse/sync");
const https = require("https");
const AdmZip = require("adm-zip");

const HTML_BUCKET = process.env.HTML_BUCKET || "my-property-data-pipeline-uploads-aya";
const OUTPUT_PREFIX = process.env.OUTPUT_PREFIX || "output/html";
const ERRORS_KEY = process.env.ERRORS_KEY || "errors.csv";
const RETRY_LIMIT = parseInt(process.env.RETRY_LIMIT || "3", 10);
const RETRY_DELAY_MS = parseInt(process.env.RETRY_DELAY_MS || "5000", 10);

let browser = null;

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function waitForModalAndDismiss(page, { timeout = 15000 } = {}) {
  // Check if the modal exists at all
  const modal = await page.$('#pnlIssues');
  if (!modal) return;

  const buttonSelector = '#pnlIssues input[name="btnContinue"]';

  // Wait for button to be visible
  await page.waitForSelector(buttonSelector, { visible: true, timeout: 5000 });
  await page.click(buttonSelector);

  // Wait for modal to disappear OR a navigation
  await Promise.race([
    page.waitForSelector('#pnlIssues', { hidden: true, timeout }),
    page.waitForNavigation({ waitUntil: 'domcontentloaded', timeout }).catch(() => {})
  ]);
}

async function waitForPropertyData(page, { timeout = 20000 } = {}) {
  await page.waitForSelector(
    '#parcelLabel, .sectionTitle, table.detailsTable, .textPanel, [id*="Property"]',
    { timeout }
  );
}



async function getPublicIP() {
  return new Promise((resolve, reject) => {
    https.get("https://checkip.amazonaws.com", (res) => {
      let ip = "";
      res.on("data", (chunk) => { ip += chunk; });
      res.on("end", () => resolve(ip.trim()));
    }).on("error", (err) => reject(err));
  });
}

async function initializeBrowser() {
  if (!browser) {
    browser = await puppeteer.launch({
      ignoreDefaultArgs: ['--disable-extensions'],
      executablePath: await chromium.executablePath(),
      headless: chromium.headless,
      defaultViewport: chromium.defaultViewport,
      args: [
        ...chromium.args,
        '--hide-scrollbars',
        '--disable-web-security',
        '--no-sandbox'
      ],
      // CHANGED: give Chromium time to start on cold starts (or omit entirely)
      timeout: 30000,
    });
  }
  return browser;
}

function formatUtcTimestamp() {
  const d = new Date();
  const pad = (n) => String(n).padStart(2, '0');
  const y = d.getUTCFullYear();
  const m = pad(d.getUTCMonth() + 1);
  const day = pad(d.getUTCDate());
  const hh = pad(d.getUTCHours());
  const mm = pad(d.getUTCMinutes());
  const ss = pad(d.getUTCSeconds());
  return `${y}${m}${day}T${hh}${mm}${ss}Z`;
}

async function uploadZipToS3(parcelId, zipBuffer) {
  const ts = formatUtcTimestamp();
  const key = `${OUTPUT_PREFIX}/${parcelId}-${ts}.zip`;
  await S3.putObject({
    Bucket: HTML_BUCKET,
    Key: key,
    Body: zipBuffer,
    ContentType: "application/zip"
  }).promise();
  console.log(`✅ Uploaded zip to s3://${HTML_BUCKET}/${key}`);
}

async function loadCsvFromS3(s3Key) {
  const params = { Bucket: HTML_BUCKET, Key: s3Key };
  const data = await S3.getObject(params).promise();
  return parse(data.Body.toString("utf-8"), {
    columns: true,
    skip_empty_lines: true
  });
}

function buildUrlFromRow(row) {
  const baseUrl = (row.url || "").trim();
  const multiValueRaw = (row.multiValueQueryString || "").trim();
  if (!baseUrl) return "";
  try {
    const urlObj = new URL(baseUrl);
    if (!multiValueRaw) return urlObj.toString();

    // Try JSON first (e.g., { "key": ["v1", "v2"], "x": ["y"] })
    let appended = false;
    if (multiValueRaw.startsWith("{") || multiValueRaw.startsWith("[")) {
      try {
        const parsed = JSON.parse(multiValueRaw);
        if (parsed && typeof parsed === "object") {
          for (const [key, value] of Object.entries(parsed)) {
            if (Array.isArray(value)) {
              for (const v of value) {
                urlObj.searchParams.append(key, String(v));
              }
            } else if (value != null) {
              urlObj.searchParams.append(key, String(value));
            }
          }
          appended = true;
        }
      } catch (_) {
        // fall through to querystring parsing
      }
    }

    if (!appended) {
      // Treat as standard querystring: a=1&b=2&c=3
      const params = new URLSearchParams(multiValueRaw);
      for (const [k, v] of params.entries()) {
        urlObj.searchParams.append(k, v);
      }
    }
    return urlObj.toString();
  } catch (_e) {
    // If base URL is not fully qualified, return as-is with raw query appended
    if (!multiValueRaw) return baseUrl;
    const joiner = baseUrl.includes("?") ? "&" : "?";
    return `${baseUrl}${joiner}${multiValueRaw}`;
  }
}

function escapeCsvValue(value) {
  const str = value == null ? '' : String(value);
  const needsQuotes = /[",\n]/.test(str);
  const escaped = str.replace(/"/g, '""');
  return needsQuotes ? `"${escaped}"` : escaped;
}

function rowToCsvLine(headers, row) {
  return headers.map(h => escapeCsvValue(row[h])).join(',');
}

async function appendRowsToErrorsCsv(headers, rows) {
  if (!rows || rows.length === 0) return;
  let existing = '';
  let hasExisting = false;
  try {
    const res = await S3.getObject({ Bucket: HTML_BUCKET, Key: ERRORS_KEY }).promise();
    existing = res.Body.toString('utf-8');
    hasExisting = true;
  } catch (e) {
    const code = e.code || (e.$metadata && e.$metadata.httpStatusCode);
    hasExisting = false;
  }

  let output = '';
  if (!hasExisting || existing.trim().length === 0) {
    output += headers.join(',') + '\n';
  } else {
    output += existing;
    if (!existing.endsWith('\n')) {
      output += '\n';
    }
  }

  for (const r of rows) {
    output += rowToCsvLine(headers, r) + '\n';
  }

  await S3.putObject({
    Bucket: HTML_BUCKET,
    Key: ERRORS_KEY,
    Body: Buffer.from(output, 'utf-8'),
    ContentType: 'text/csv'
  }).promise();
}

async function scrapeOnPage(page, parcelID, url) {
  try {
    await page.evaluateOnNewDocument(() => {
      Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
    });

    await page.setUserAgent("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36");
    await page.setExtraHTTPHeaders({
      "Accept-Language": "en-US,en;q=0.9",
      "Accept": "text/html,application/xhtml+xml"
    });

    console.log(`🌐 Navigating to: ${url}`);
    await page.goto(url, { waitUntil: "networkidle2", timeout: 60000 });

    // Wait for either the issues modal to appear or any property data indicator to show up
    await Promise.race([
      page.waitForSelector('#pnlIssues', { visible: true, timeout: 8000 }).catch(() => {}),
      page.waitForFunction(() => {
        return (
          document.querySelector('#parcelLabel') ||
          document.querySelector('.sectionTitle') ||
          document.querySelector('table.detailsTable') ||
          document.querySelector('.textPanel') ||
          document.querySelector('[id*="Property"]')
        );
      }, { timeout: 15000 }).catch(() => {})
    ]);

    // More specific continue button detection
    const continueButtonInfo = await page.evaluate(() => {
      // Look for the specific modal popup that contains the continue button
      const modal = document.getElementById('pnlIssues');
      if (!modal) return null;

      const modalStyle = window.getComputedStyle(modal);
      const isModalVisible = modalStyle.display !== 'none' &&
                           modalStyle.visibility !== 'hidden' &&
                           modalStyle.zIndex > 0;

      if (!isModalVisible) return null;

      // Look for the specific continue button within the modal
      const continueButton = modal.querySelector('input[name="btnContinue"]') ||
                           modal.querySelector('input[value="Continue"]') ||
                           modal.querySelector('button[value="Continue"]');

      if (!continueButton) return null;

      return {
        found: true,
        modalId: 'pnlIssues',
        buttonSelector: continueButton.name ? `input[name="${continueButton.name}"]` : 'input[value="Continue"]',
        buttonText: continueButton.value || continueButton.textContent
      };
    });

    if (continueButtonInfo && continueButtonInfo.found) {
      console.log(`🔘 Continue modal detected for ${parcelID}, clicking continue button`);

      try {
        // Wait for the specific continue button to be clickable
        await page.waitForSelector(continueButtonInfo.buttonSelector, {
          visible: true,
          timeout: 5000
        });

        // Click the continue button
        await page.click(continueButtonInfo.buttonSelector);
        console.log(`✅ Continue button clicked for ${parcelID}`);

        // Wait for navigation or content change
        try {
          await Promise.race([
            page.waitForNavigation({
              waitUntil: "networkidle2",
              timeout: 30000
            }),
            page.waitForFunction(() => {
              const modal = document.getElementById('pnlIssues');
              if (!modal) return true;
              const style = window.getComputedStyle(modal);
              return style.display === 'none' || style.visibility === 'hidden';
            }, { timeout: 30000 })
          ]);
        } catch (waitError) {
          console.log(`⚠️ Wait timeout for ${parcelID}, but continuing...`);
        }

        // Check if we need to wait for more content
        await page.waitForFunction(() => {
          // Look for property data indicators
          const propertyData = document.querySelector('#parcelLabel') ||
                             document.querySelector('.sectionTitle') ||
                             document.querySelector('[id*="Property"]');
          return propertyData !== null;
        }, { timeout: 15000 }).catch(() => {
          console.log(`⚠️ Property data elements not found quickly for ${parcelID}`);
        });

      } catch (clickError) {
        console.error(`❌ Error clicking continue button for ${parcelID}: ${clickError.message}`);
        // Continue anyway, might still get some data
      }
    } else {
      console.log(`ℹ️ No continue modal detected for ${parcelID}`);
    }

    // Final ensure: wait a bit for any property data indicator if not already present
    await page.waitForFunction(() => {
      return (
        document.querySelector('#parcelLabel') ||
        document.querySelector('.sectionTitle') ||
        document.querySelector('table.detailsTable') ||
        document.querySelector('.textPanel') ||
        document.querySelector('[id*="Property"]')
      );
    }, { timeout: 5000 }).catch(() => {});

    // Extract content
    const html = await page.content();

    // Verify we have meaningful content
    const hasPropertyData = await page.evaluate(() => {
      // Check for key indicators that we have property data
      const indicators = [
        document.querySelector('#parcelLabel'),
        document.querySelector('.sectionTitle'),
        document.querySelector('[id*="Property"]'),
        document.querySelector('table.detailsTable'),
        document.querySelector('.textPanel')
      ];

      return indicators.some(el => el !== null);
    });

    if (hasPropertyData) {
      console.log(`✅ Property data found for ${parcelID}`);
      return html;
    } else {
      console.log(`⚠️ Limited property data found for ${parcelID}`);
      // Signal to caller to retry this parcel
      const err = new Error("LIMITED_PROPERTY_DATA");
      err.code = "LIMITED_PROPERTY_DATA";
      throw err;
    }

  } finally {
    // Do not close the page here; caller manages lifecycle for reuse across retries
  }
}

// Backward-compatible wrapper; kept in case other callers rely on previous behavior
async function scrapeWithPuppeteer(parcelID, url) {
  const browser = await initializeBrowser();
  const page = await browser.newPage();
  try {
    return await scrapeOnPage(page, parcelID, url);
  } finally {
    if (page && !page.isClosed()) {
      await page.close().catch(() => {});
    }
  }
}


exports.handler = async (event) => {
  console.log("🚀 Lambda triggered:", JSON.stringify(event));

  try {
    const ip = await getPublicIP();
    console.log(`🌐 Lambda public IP: ${ip}`);
  } catch (err) {
    console.warn("⚠️ Could not retrieve public IP:", err.message);
  }

  let totalSuccess = 0, totalFailed = 0;

  const batchItemFailures = [];

  // Process each SQS record
  for (const record of event.Records) {
    let recordShouldFail = false;
    try {
      const msg = JSON.parse(record.body);
      const batchKey = msg.s3_key;
      if (!batchKey) {
        console.warn("⚠️ Missing s3_key");
        continue;
      }

      console.log(`📥 Fetching CSV from: s3://${HTML_BUCKET}/${batchKey}`);
      const rows = await loadCsvFromS3(batchKey);

      if (rows.length === 0) {
        console.warn("⚠️ Empty CSV batch.");
        continue;
      }

      let success = 0, failed = 0;
      let headersForErrors = null;
      const failedRowsBuffer = [];

      for (const row of rows) {
        if (!headersForErrors) {
          headersForErrors = Object.keys(row);
        }
        const { parcel_id } = row;
        const url = buildUrlFromRow(row);

        if (!url) {
          console.warn(`⚠️ Missing URL for parcel ${parcel_id}`);
          failed++;
          continue;
        }

        const browserInstance = await initializeBrowser();
        const page = await browserInstance.newPage();
        try {
          let attempt = 0;
          let completed = false;
          let html = null;
          while (attempt < RETRY_LIMIT && !completed) {
            try {
              html = await scrapeOnPage(page, parcel_id, url);
              success++;
              completed = true;
            } catch (err) {
              const isLimited = err && (err.code === "LIMITED_PROPERTY_DATA" || err.message === "LIMITED_PROPERTY_DATA");
              attempt++;
              if (isLimited && attempt < RETRY_LIMIT) {
                const backoff = RETRY_DELAY_MS * attempt;
                console.warn(`↻ Retry ${attempt}/${RETRY_LIMIT} for parcel ${parcel_id} due to limited data. Waiting ${backoff}ms...`);
                await sleep(backoff);
                continue;
              }
              console.error(`❌ Scrape failed for ${parcel_id} (attempt ${attempt}): ${err && err.message}`);
              failed++;
              if (isLimited && attempt >= RETRY_LIMIT) {
                // Mark entire SQS record to be retried/ DLQ'd after SQS redrive
                recordShouldFail = true;
              }
              // capture failed row for errors.csv
              failedRowsBuffer.push(row);
              break;
            }
          }

          if (completed && html !== null) {
            // Create seed.csv for this row with headers
            const headers = Object.keys(row);
            const csvHeader = headers.join(',') + '\n';
            const csvRow = headers.map(h => {
              const val = row[h] == null ? '' : String(row[h]);
              const needsQuotes = /[",\n]/.test(val);
              const escaped = val.replace(/"/g, '""');
              return needsQuotes ? `"${escaped}"` : escaped;
            }).join(',') + '\n';

            const zip = new AdmZip();
            zip.addFile('seed.csv', Buffer.from(csvHeader + csvRow, 'utf-8'));
            zip.addFile(`${parcel_id}.html`, Buffer.from(html, 'utf-8'));
            const zipBuffer = zip.toBuffer();
            await uploadZipToS3(parcel_id, zipBuffer);
          }
        } finally {
          if (page && !page.isClosed()) {
            await page.close().catch(() => {});
          }
        }
      }

      console.log(`✅ Batch complete. Success: ${success}, Failed: ${failed}`);
      if (failedRowsBuffer.length > 0 && headersForErrors) {
        try {
          await appendRowsToErrorsCsv(headersForErrors, failedRowsBuffer);
          console.log(`📄 Appended ${failedRowsBuffer.length} failed row(s) to s3://${HTML_BUCKET}/${ERRORS_KEY}`);
        } catch (e) {
          console.warn(`⚠️ Failed to append errors.csv: ${e && e.message}`);
        }
      }
      totalSuccess += success;
      totalFailed += failed;

    } catch (err) {
      console.error(`❌ Error processing record: ${err.message}`);
      totalFailed++;
      recordShouldFail = true;
    }

    if (recordShouldFail && record && record.messageId) {
      batchItemFailures.push({ itemIdentifier: record.messageId });
    }
  }

  // Clean up browser
  if (browser) {
    try {
      await browser.close();
      console.log("🔒 Browser closed.");
    } catch (e) {
      console.warn("⚠️ Error closing browser:", e.message);
    }
    browser = null;
  }

  console.log(`🎯 Function complete. Total Success: ${totalSuccess}, Total Failed: ${totalFailed}`);

  // Enable partial batch failures so SQS can retry/route to DLQ
  if (batchItemFailures.length > 0) {
    return { batchItemFailures };
  }

  return {};

};




// Expose test helpers when running under test
if ((process.env.NODE_ENV || '').toLowerCase() === 'test') {
  module.exports._test = {
    buildUrlFromRow,
    escapeCsvValue,
    rowToCsvLine,
    appendRowsToErrorsCsv,
  };
}