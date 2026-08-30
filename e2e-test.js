const { chromium } = require('/usr/local/lib/node_modules/@playwright/cli/node_modules/playwright');

(async () => {
  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage();

  console.log('1. Loading page...');
  await page.goto('http://localhost:3000', { waitUntil: 'networkidle' });

  const title = await page.title();
  console.log(`   Title: ${title}`);

  const metaDesc = await page.$eval('meta[name="description"]', el => el.content);
  console.log(`   Meta description: ${metaDesc.substring(0, 50)}...`);

  const mainLandmark = await page.$('main');
  console.log(`   <main> landmark: ${mainLandmark ? 'present' : 'MISSING'}`);

  const logoImg = await page.$('img.logo-img');
  const width = await logoImg.getAttribute('width');
  const height = await logoImg.getAttribute('height');
  const fetchPri = await logoImg.getAttribute('fetchpriority');
  const loading = await logoImg.getAttribute('loading');
  console.log(`   Logo: ${width}x${height}, fetchpriority=${fetchPri}, loading=${loading}`);

  const ogTitle = await page.$eval('meta[property="og:title"]', el => el.content);
  console.log(`   OG title: ${ogTitle.substring(0, 50)}...`);

  console.log('\n2. Testing conversion flow...');
  await page.fill('#urlInput', 'https://music.youtube.com/watch?v=HgGzn4sgEqc');
  await page.click('#convertBtn');

  console.log('   Waiting for progress...');
  await page.waitForSelector('.progress-container.visible', { timeout: 10000 });
  console.log('   Progress container visible');

  let status = '';
  for (let i = 0; i < 40; i++) {
    await page.waitForTimeout(2000);
    const progressText = await page.$eval('#progressText', el => el.textContent);
    console.log(`   [${i * 2}s] ${progressText}`);

    const downloadVisible = await page.$eval('#downloadBtn', el =>
      el.classList.contains('visible')
    );
    if (downloadVisible) {
      console.log('   Download button visible!');
      status = 'completed';
      break;
    }

    const errorVisible = await page.$eval('#errorMsg', el =>
      el.classList.contains('visible')
    );
    if (errorVisible) {
      const errorText = await page.$eval('#errorMsg', el => el.textContent);
      console.log(`   ERROR: ${errorText}`);
      status = 'failed';
      break;
    }
  }

  console.log(`\n3. Result: ${status}`);
  await browser.close();
  process.exit(status === 'completed' ? 0 : 1);
})();
