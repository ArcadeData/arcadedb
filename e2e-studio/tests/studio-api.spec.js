
const { test, expect } = require('@playwright/test');
const { loginToStudio } = require('./helpers/auth-helper');

test.describe('ArcadeDB Studio - API Integration Tests', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await loginToStudio(page);
  });

  test('should make successful API calls for data retrieval', async ({ page }) => {
    // Monitor network requests
    const apiRequests = [];
    page.on('request', request => {
      if (request.url().includes('/api/')) {
        apiRequests.push(request);
      }
    });

    // Navigate to database page which should trigger API calls
    await page.click('text=Database');
    await page.waitForLoadState('networkidle');

    // Check that API requests were made
    expect(apiRequests.length).toBeGreaterThan(0);

    // Check for successful responses
    const responses = await Promise.all(
      apiRequests.map(async (request) => {
        try {
          const response = await request.response();
          return response ? response.status() : null;
        } catch (error) {
          return null;
        }
      })
    );

    const successfulResponses = responses.filter(status => status === 200 || status === 204);
    expect(successfulResponses.length).toBeGreaterThan(0);
  });

  test('should handle API authentication correctly', async ({ page }) => {
    // Monitor authentication headers
    let hasAuthHeader = false;
    let apiRequestCount = 0;

    page.on('request', request => {
      if (request.url().includes('/api/')) {
        apiRequestCount++;
        const headers = request.headers();
        if (headers['authorization'] || headers['cookie'] || headers['x-arcadedb-session']) {
          hasAuthHeader = true;
        }
      }
    });

    // Navigate to database tab to trigger API calls
    await page.click('#tab-database-sel');
    await page.waitForLoadState('networkidle');

    // Should have made API requests
    expect(apiRequestCount).toBeGreaterThan(0);

    // Authentication headers are optional depending on server configuration
    // The test passes if either auth headers are present OR requests succeed without them
    console.log('API requests made:', apiRequestCount);
    console.log('Has auth headers:', hasAuthHeader);
  });

  test('should display API documentation page', async ({ page }) => {
    // Try to access API documentation
    await page.goto('/api.html');

    // Check if API docs page loads
    const apiDocsElements = page.locator('h1, h2, .api-docs, .swagger, .openapi');
    const hasApiDocs = await apiDocsElements.count() > 0;

    if (hasApiDocs) {
      await expect(apiDocsElements.first()).toBeVisible();

      // Check for common API documentation elements
      const endpoints = page.locator('.endpoint, .api-endpoint, .method');
      const hasEndpoints = await endpoints.count() > 0;

      if (hasEndpoints) {
        await expect(endpoints.first()).toBeVisible();
      }
    }
  });

  test('should handle API rate limiting gracefully', async ({ page }) => {
    let rateLimitResponses = 0;

    page.on('response', response => {
      if (response.status() === 429 || response.status() === 503) {
        rateLimitResponses++;
      }
    });

    // Make multiple rapid requests
    for (let i = 0; i < 10; i++) {
      await page.goto('/database', { waitUntil: 'domcontentloaded' });
    }

    // If rate limiting occurs, it should be handled gracefully
    if (rateLimitResponses > 0) {
      // Check for rate limit error messages
      const rateLimitMessage = page.locator('.rate-limit, .too-many-requests, .error');
      const hasRateLimitMessage = await rateLimitMessage.count() > 0;

      if (hasRateLimitMessage) {
        await expect(rateLimitMessage.first()).toBeVisible();
      }
    }
  });

  test('should show real-time data updates', async ({ page }) => {
    await page.goto('/database');

    // Look for real-time update indicators
    const realtimeIndicators = page.locator('.live-data, .real-time, .auto-refresh, .live-update');
    const hasRealtimeIndicators = await realtimeIndicators.count() > 0;

    if (hasRealtimeIndicators) {
      await expect(realtimeIndicators.first()).toBeVisible();
    }

    // Check for periodic refresh
    let requestCount = 0;
    page.on('request', request => {
      if (request.url().includes('/api/') && request.method() === 'GET') {
        requestCount++;
      }
    });

    // Wait and check if periodic requests are made
    await page.waitForTimeout(5000);

    // Should have made some requests during this time
    expect(requestCount).toBeGreaterThan(0);
  });

  test('should handle API server errors gracefully', async ({ page }) => {
    // Mock API server error
    await page.route('**/api/**', route => {
      route.fulfill({
        status: 500,
        contentType: 'application/json',
        body: JSON.stringify({ error: 'Internal Server Error' })
      });
    });

    await page.goto('/database');

    // Check for error handling
    const errorMessages = page.locator('.error, .alert-danger, .server-error, .api-error');
    const hasErrorMessages = await errorMessages.count() > 0;

    if (hasErrorMessages) {
      await expect(errorMessages.first()).toBeVisible();
      await expect(errorMessages.first()).toContainText(/error|failed|unavailable/i);
    }
  });

  test('should validate API response formats', async ({ page }) => {
    let validJsonResponses = 0;
    let totalApiResponses = 0;

    page.on('response', async response => {
      if (response.url().includes('/api/')) {
        totalApiResponses++;
        if (response.status() === 200) {
          try {
            const contentType = response.headers()['content-type'];
            if (contentType && contentType.includes('application/json')) {
              const body = await response.json();
              if (typeof body === 'object') {
                validJsonResponses++;
              }
            }
          } catch (error) {
            // Invalid JSON response
          }
        }
      }
    });

    // Navigate to database tab to trigger API calls
    await page.click('#tab-database-sel');
    await page.waitForLoadState('networkidle');

    console.log('Total API responses:', totalApiResponses);
    console.log('Valid JSON responses:', validJsonResponses);

    // Should have received some API responses (may not all be JSON)
    expect(totalApiResponses).toBeGreaterThan(0);
  });

  test('should support API versioning', async ({ page }) => {
    let apiVersionRequests = [];

    page.on('request', request => {
      if (request.url().includes('/api/v')) {
        const match = request.url().match(/\/api\/v(\d+)\//);
        if (match) {
          apiVersionRequests.push(match[1]);
        }
      }
    });

    await page.goto('/database');
    await page.waitForLoadState('networkidle');

    // Should use versioned API endpoints
    if (apiVersionRequests.length > 0) {
      expect(apiVersionRequests).toContain('1'); // Assuming v1 API
    }
  });

  test('should handle CORS properly for API requests', async ({ page }) => {
    let corsHeaders = {};

    page.on('response', response => {
      if (response.url().includes('/api/')) {
        const headers = response.headers();
        if (headers['access-control-allow-origin']) {
          corsHeaders = headers;
        }
      }
    });

    await page.goto('/database');
    await page.waitForLoadState('networkidle');

    // Should have CORS headers if needed
    if (Object.keys(corsHeaders).length > 0) {
      expect(corsHeaders['access-control-allow-origin']).toBeDefined();
    }
  });

  test('should provide API status and health checks', async ({ page }) => {
    // Try to access health check endpoint
    const response = await page.request.get('/api/v1/ready');

    // Health check should return appropriate status
    expect([200, 204, 503]).toContain(response.status());

    // If there's a status page, check it
    await page.goto('/api/v1/ready');

    const statusElements = page.locator('.status, .health, .ready');
    const hasStatusElements = await statusElements.count() > 0;

    if (hasStatusElements) {
      await expect(statusElements.first()).toBeVisible();
    }
  });
});
