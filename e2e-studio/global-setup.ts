import { chromium, FullConfig } from '@playwright/test';

async function globalSetup(config: FullConfig) {
  console.log('🔍 Checking if ArcadeDB server is available...');

  const browser = await chromium.launch();
  const page = await browser.newPage();

  try {
    // Try to connect to the server with retries
    const maxRetries = 30; // 30 retries = 60 seconds max wait
    let retries = 0;

    while (retries < maxRetries) {
      try {
        console.log(`⏳ Attempt ${retries + 1}/${maxRetries} - Checking server connectivity...`);

        // Try to reach the health endpoint first
        const response = await page.request.get('http://localhost:2480/api/v1/ready', {
          timeout: 2000
        });

        if (response.status() === 204) {
          console.log('✅ Server health check passed');

          // Now try to load the Studio interface
          await page.goto('http://localhost:2480', { timeout: 5000 });
          console.log('✅ ArcadeDB Studio is accessible');
          break;
        }
      } catch (error) {
        retries++;
        if (retries >= maxRetries) {
          console.error('❌ Failed to connect to ArcadeDB server after maximum retries');
          console.error('🔧 Make sure ArcadeDB server is running on http://localhost:2480');
          console.error('💡 Original error:', error.message);
          throw new Error('ArcadeDB server is not accessible');
        }

        console.log(`⏱️  Server not ready, waiting 2 seconds before retry...`);
        await new Promise(resolve => setTimeout(resolve, 2000));
      }
    }
  } finally {
    await browser.close();
  }

  console.log('🚀 Global setup completed successfully');
}

export default globalSetup;
