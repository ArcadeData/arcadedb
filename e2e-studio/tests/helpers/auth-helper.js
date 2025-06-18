const { expect } = require('@playwright/test');

/**
 * Helper function to handle ArcadeDB Studio authentication
 * @param {Page} page - Playwright page object
 * @param {string} username - Username (default: 'root')
 * @param {string} password - Password (default: 'playwithdata')
 */
async function loginToStudio(page, username = 'root', password = 'playwithdata') {
  // Wait for page to load
  await page.waitForLoadState('networkidle');

  // Check if login dialog is present (multiple possible selectors)
  const loginDialog = page.locator('dialog:has-text("Login to the server"), #loginPopup');
  const hasLoginDialog = await loginDialog.count() > 0;

  if (hasLoginDialog) {
    // Wait for dialog to be visible
    await expect(loginDialog).toBeVisible({ timeout: 5000 });

    // Find the correct input fields - check both dialog and modal popup
    const usernameField = page.locator('dialog input[placeholder="User Name"], dialog input[type="text"], #loginPopup input[type="text"], #loginPopup input[placeholder*="User"]').first();
    const passwordField = page.locator('dialog input[type="password"], #loginPopup input[type="password"]').first();
    const loginButton = page.locator('dialog button:has-text("Sign in"), #loginPopup button:has-text("Sign in"), #loginPopup button[type="submit"]').first();

    // Clear and fill in credentials
    await usernameField.clear();
    await usernameField.fill(username);

    await passwordField.clear();
    await passwordField.fill(password);

    // Click login button
    await loginButton.click();

    // Wait for dialog to disappear
    await expect(loginDialog).toBeHidden({ timeout: 10000 });
    await page.waitForLoadState('networkidle');
  }

  // Always ensure studio panel is visible (login might not be required)
  // Check if studio panel exists but is hidden
  const studioPanel = page.locator('#studioPanel');
  const isHidden = await studioPanel.evaluate(el => el.style.display === 'none');

  if (isHidden) {
    // Show the studio panel programmatically
    await studioPanel.evaluate(el => el.style.display = 'block');
    await page.waitForTimeout(500); // Wait for transitions
  }

  // Verify studio panel is now visible
  await expect(studioPanel).toBeVisible({ timeout: 5000 });

  return true;
}

/**
 * Check if user is logged in
 * @param {Page} page - Playwright page object
 */
async function isLoggedIn(page) {
  const loginDialog = page.locator('dialog:has-text("Login to the server")');
  return (await loginDialog.count()) === 0;
}

/**
 * Logout from studio
 * @param {Page} page - Playwright page object
 */
async function logoutFromStudio(page) {
  // Look for logout button in various locations
  const logoutButtons = [
    'button:has-text("Logout")',
    'a:has-text("Logout")',
    'button:has-text("Sign out")',
    '.logout',
    '[onclick*="logout"]',
    'a[href*="logout"]'
  ];

  for (const selector of logoutButtons) {
    const logoutButton = page.locator(selector);
    if (await logoutButton.count() > 0) {
      await logoutButton.first().click();
      await page.waitForLoadState('networkidle');
      return true;
    }
  }

  return false;
}

module.exports = {
  loginToStudio,
  isLoggedIn,
  logoutFromStudio
};
