import { test, expect } from '@playwright/test';

test.describe('ArcadeDB Studio Database Creation', () => {
  test('should create a new database called e2e-studio', async ({ page }) => {
    // Navigate to ArcadeDB Studio
    await page.goto('http://localhost:2480');

    // Wait for login dialog to appear
    await expect(page.getByRole('dialog', { name: 'Login to the server' })).toBeVisible();

    // Fill in login credentials
    await page.getByRole('textbox', { name: 'User Name' }).fill('root');
    await page.getByRole('textbox', { name: 'Password' }).fill('playwithdata');

    // Click sign in button
    await page.getByRole('button', { name: 'Sign in' }).click();

    // Wait for the main interface to load
    await expect(page.getByText('Connected as').first()).toBeVisible();

    // Navigate to Database tab (second tab with database icon)
    await page.getByRole('tab').nth(1).click();

    // Verify we're on the Database tab
    await expect(page.getByRole('heading', { name: 'Database' })).toBeVisible();

    // Click the Create button to open database creation dialog
    await page.getByRole('button', { name: '+ Create' }).click();

    // Wait for the creation dialog to appear
    await expect(page.getByRole('dialog', { name: 'Create a new database' })).toBeVisible();

    // Enter the database name
    await page.getByRole('textbox', { name: 'Enter the database name:' }).fill('e2e-studio');

    // Click Send to create the database
    await page.getByRole('button', { name: 'Send' }).click();

    // Wait for the dialog to close
    await expect(page.getByRole('dialog', { name: 'Create a new database' })).not.toBeVisible();

    // Verify the new database is selected in the dropdown
    await expect(page.locator('select').first()).toContainText('e2e-studio');

    // Verify we can see the database schema section
    await expect(page.getByText('Database Schema')).toBeVisible();
  });
});
