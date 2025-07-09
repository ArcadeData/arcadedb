import { FullConfig } from '@playwright/test';

async function globalTeardown(config: FullConfig) {
  console.log('🧹 Cleaning up ArcadeDB container...');

  try {
    const arcadedbContainer = global.arcadedbContainer;

    if (arcadedbContainer) {
      await arcadedbContainer.stop();
      console.log('✅ ArcadeDB container stopped successfully');
    } else {
      console.log('ℹ️ No container to stop');
    }
  } catch (error) {
    console.error('❌ Error stopping ArcadeDB container:', error);
  }

  console.log('🚀 Global teardown completed');
}

export default globalTeardown;
