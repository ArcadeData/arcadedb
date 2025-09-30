#!/usr/bin/env node
/**
 * Copy Swagger UI distribution files to static resources
 *
 * This script copies essential Swagger UI files from node_modules
 * to the Spring Boot static resources directory for serving.
 */

const fs = require('fs');
const path = require('path');

// Configuration
const SOURCE_DIR = path.join(__dirname, '..', 'node_modules', 'swagger-ui-dist');
const TARGET_DIR = path.join(__dirname, '..', 'target', 'classes','static',  'swagger-ui');

// Files to copy from swagger-ui-dist
const FILES_TO_COPY = [
  'swagger-ui-bundle.js',
  'swagger-ui-standalone-preset.js',
  'swagger-ui.css',
  'favicon-32x32.png',
  'oauth2-redirect.html'
];

/**
 * Ensure directory exists, create if it doesn't
 * @param {string} dirPath - Directory path to create
 */
function ensureDirectoryExists(dirPath) {
  if (!fs.existsSync(dirPath)) {
    console.log(`Creating directory: ${dirPath}`);
    fs.mkdirSync(dirPath, { recursive: true });
  }
}

/**
 * Copy a single file from source to target
 * @param {string} fileName - Name of the file to copy
 * @returns {boolean} - Success status
 */
function copyFile(fileName) {
  const sourcePath = path.join(SOURCE_DIR, fileName);
  const targetPath = path.join(TARGET_DIR, fileName);

  try {
    // Check if source file exists
    if (!fs.existsSync(sourcePath)) {
      console.error(`ERROR: Source file not found: ${sourcePath}`);
      return false;
    }

    // Copy the file
    fs.copyFileSync(sourcePath, targetPath);

    // Verify the copy
    const sourceSize = fs.statSync(sourcePath).size;
    const targetSize = fs.statSync(targetPath).size;

    if (sourceSize !== targetSize) {
      console.error(`ERROR: File size mismatch for ${fileName}`);
      return false;
    }

    console.log(`Copied: ${fileName} (${formatBytes(sourceSize)})`);
    return true;

  } catch (error) {
    console.error(`ERROR: Failed to copy ${fileName}: ${error.message}`);
    return false;
  }
}

/**
 * Format bytes to human-readable format
 * @param {number} bytes - Number of bytes
 * @returns {string} - Formatted string
 */
function formatBytes(bytes) {
  if (bytes === 0) return '0 Bytes';
  const k = 1024;
  const sizes = ['Bytes', 'KB', 'MB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return Math.round(bytes / Math.pow(k, i) * 100) / 100 + ' ' + sizes[i];
}

/**
 * Main execution function
 */
function main() {
  console.log('======================================');
  console.log('Swagger UI Copy Script');
  console.log('======================================');
  console.log(`Source: ${SOURCE_DIR}`);
  console.log(`Target: ${TARGET_DIR}`);
  console.log('======================================\n');

  // Check if source directory exists
  if (!fs.existsSync(SOURCE_DIR)) {
    console.error('ERROR: swagger-ui-dist package not found in node_modules');
    console.error('Please run: npm install swagger-ui-dist');
    process.exit(1);
  }

  // Ensure target directory exists
  try {
    ensureDirectoryExists(TARGET_DIR);
  } catch (error) {
    console.error(`ERROR: Failed to create target directory: ${error.message}`);
    process.exit(1);
  }

  // Copy all files
  let successCount = 0;
  let failureCount = 0;

  console.log('Copying files...\n');

  for (const fileName of FILES_TO_COPY) {
    if (copyFile(fileName)) {
      successCount++;
    } else {
      failureCount++;
    }
  }

  // Summary
  console.log('\n======================================');
  console.log('Copy Summary');
  console.log('======================================');
  console.log(`Total files: ${FILES_TO_COPY.length}`);
  console.log(`Successfully copied: ${successCount}`);
  console.log(`Failed: ${failureCount}`);
  console.log('======================================\n');

  // Exit with appropriate code
  if (failureCount > 0) {
    console.error('Some files failed to copy. Please check the errors above.');
    process.exit(1);
  } else {
    console.log('All Swagger UI files copied successfully!');
    process.exit(0);
  }
}

// Execute main function
if (require.main === module) {
  main();
}

module.exports = { copyFile, ensureDirectoryExists };
