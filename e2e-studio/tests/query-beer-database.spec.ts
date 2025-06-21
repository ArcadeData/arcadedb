import { test, expect } from '@playwright/test';

test.describe('ArcadeDB Studio Beer Database Query', () => {
  test('should query Beer database and display 10 vertices in graph tab', async ({ page }) => {
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

    // Select the Beer database from the dropdown
    await page.getByLabel('root').selectOption('Beer');

    // Verify Beer database is selected
    await expect(page.getByLabel('root')).toHaveValue('Beer');

    // Make sure we're on the Query tab (first tab should be selected by default)
    // Wait for the query interface to be visible
    await expect(page.getByText('Auto Limit')).toBeVisible();

    // Wait for the visible query textarea in the main tab panel and enter the SQL query
    const queryTextarea = page.getByRole('tabpanel').getByRole('textbox');
    await expect(queryTextarea).toBeVisible();
    await queryTextarea.fill('SELECT FROM Beer LIMIT 10');

    // Execute the query by clicking the execute button
    await page.getByRole('button', { name: '' }).first().click();

    // Wait for query results to load
    await expect(page.getByText('Returned')).toBeVisible();
    await expect(page.getByText('records in')).toBeVisible();

    // Verify that 10 records were returned
    await expect(page.getByText('10', { exact: true }).first()).toBeVisible();

    // Verify we're on the Graph tab (it should be selected by default)
    await expect(page.getByRole('link', { name: 'Graph' })).toBeVisible();

    // Verify that the graph displays exactly 10 vertices
    await expect(page.getByText('Displayed')).toBeVisible();
    await expect(page.getByText('vertices and')).toBeVisible();

    // Check specifically for "Displayed 10 vertices and 0 edges"
    const graphStats = page.locator('text=Displayed').locator('..'); // Get parent element
    await expect(graphStats).toContainText('Displayed 10 vertices and 0 edges');

    // Alternative verification: check for the exact count in the graph stats
    await expect(page.getByText('10').nth(1)).toBeVisible(); // Second occurrence should be in graph stats
  });
});
