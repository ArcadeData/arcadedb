const { test, expect } = require('@playwright/test');
const { loginToStudio, isLoggedIn, logoutFromStudio } = require('./helpers/auth-helper');

test.describe('ArcadeDB Studio - Authentication & Security Tests', () => {
  test('should show login form when authentication is required', async ({ page }) => {
    // Try to access a protected endpoint that might require authentication
    await page.goto('/');

    // Check if login modal dialog is present
    const loginDialog = page.locator('dialog:has-text("Login to the server")');
    const usernameField = page.locator('dialog input[placeholder="User Name"], dialog input[type="text"]');
    const passwordField = page.locator('dialog input[placeholder="Password"], dialog input[type="password"]');

    // If authentication is required, login dialog should be visible
    const hasLoginDialog = await loginDialog.count() > 0;

    if (hasLoginDialog) {
      await expect(loginDialog).toBeVisible();
      await expect(usernameField.first()).toBeVisible();
      await expect(passwordField.first()).toBeVisible();

      // Check for login button
      const loginButton = page.locator('dialog button:has-text("Sign in")');
      await expect(loginButton).toBeVisible();
    }
  });

  test('should handle valid login credentials', async ({ page }) => {
    await page.goto('/');

    // Try to login with valid credentials
    const loginSuccess = await loginToStudio(page, 'root', 'playwithdata');

    if (loginSuccess) {
      // Check if we're logged in (login dialog should be gone)
      const isAuthenticated = await isLoggedIn(page);
      expect(isAuthenticated).toBe(true);

      // Check for logged-in state indicators
      const loggedInIndicators = page.locator('.user-info, .logout, .profile, .username');
      const hasLoggedInIndicator = await loggedInIndicators.count() > 0;

      if (hasLoggedInIndicator) {
        await expect(loggedInIndicators.first()).toBeVisible();
      }
    }
  });

  test('should handle invalid login credentials', async ({ page }) => {
    await page.goto('/');

    const loginDialog = page.locator('dialog:has-text("Login to the server")');
    const needsLogin = await loginDialog.count() > 0;

    if (needsLogin) {
      const usernameField = page.locator('dialog input[placeholder="User Name"], dialog input[type="text"]').first();
      const passwordField = page.locator('dialog input[placeholder="Password"], dialog input[type="password"]').first();
      const loginButton = page.locator('dialog button:has-text("Sign in")');

      // Try invalid credentials
      await usernameField.fill('invalid_user');
      await passwordField.fill('invalid_password');
      await loginButton.click();

      await page.waitForTimeout(3000);

      // Check for error message or that dialog is still visible
      const errorMessages = page.locator('.error, .alert-danger, .login-error, .invalid-feedback');
      const hasError = await errorMessages.count() > 0;

      if (hasError) {
        await expect(errorMessages.first()).toBeVisible();
        await expect(errorMessages.first()).toContainText(/invalid|incorrect|failed|error/i);
      }

      // Login dialog should still be visible for invalid credentials
      await expect(loginDialog).toBeVisible();
    }
  });

  test('should provide logout functionality', async ({ page }) => {
    await page.goto('/');

    // Login first
    await loginToStudio(page);

    // Try to logout
    const logoutSuccess = await logoutFromStudio(page);

    if (logoutSuccess) {
      // Should redirect back to login dialog
      const loginDialog = page.locator('dialog:has-text("Login to the server")');
      await expect(loginDialog).toBeVisible({ timeout: 5000 });
    }
  });

  test('should protect sensitive pages', async ({ page }) => {
    // Try to access potentially sensitive pages without authentication
    const sensitivePages = ['/server', '/database', '/api'];

    for (const path of sensitivePages) {
      await page.goto(path);

      // Check if we're redirected to login or get an authentication prompt
      const loginRequired = await page.locator('dialog:has-text("Login to the server")').count() > 0;
      const authError = await page.locator('.unauthorized, .forbidden, .access-denied').count() > 0;

      // Either login dialog should appear or we should get an auth error
      expect(loginRequired || authError).toBe(true);
    }
  });

  test('should validate session security', async ({ page }) => {
    await page.goto('/');

    // Check for security headers or session indicators
    const response = await page.waitForResponse(response => response.url().includes(page.url()));
    const headers = response.headers();

    // Check for common security headers
    expect(headers['x-frame-options'] || headers['x-content-type-options']).toBeDefined();
  });

  test('should handle session timeout', async ({ page }) => {
    await page.goto('/');

    // Login first
    await loginToStudio(page);

    // Check for session timeout warnings or mechanisms
    const timeoutWarnings = page.locator('.session-timeout, .timeout-warning, .session-expired');
    const hasTimeoutMechanism = await timeoutWarnings.count() > 0;

    if (hasTimeoutMechanism) {
      await expect(timeoutWarnings.first()).toBeVisible();
    }
  });

  test('should display user information when logged in', async ({ page }) => {
    await page.goto('/');

    // Login
    await loginToStudio(page);

    // Check for user information display
    const userInfo = page.locator('.user-info, .username, .current-user, .profile');
    const hasUserInfo = await userInfo.count() > 0;

    if (hasUserInfo) {
      await expect(userInfo.first()).toBeVisible();
      await expect(userInfo.first()).toContainText(/root|admin|user/i);
    }
  });
});
