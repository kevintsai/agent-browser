#!/usr/bin/env node

/**
 * Copies the compiled Rust binary to bin/ with platform-specific naming
 */

import { copyFileSync, existsSync, mkdirSync, rmSync } from 'fs';
import { dirname, join } from 'path';
import { fileURLToPath } from 'url';
import { platform, arch } from 'os';

const __dirname = dirname(fileURLToPath(import.meta.url));
const projectRoot = join(__dirname, '..');

const sourceExt = platform() === 'win32' ? '.exe' : '';
const sourcePath = join(projectRoot, `cli/target/release/agent-browser${sourceExt}`);
const binDir = join(projectRoot, 'bin');

// Determine platform suffix
const platformKey = `${platform()}-${arch()}`;
const ext = platform() === 'win32' ? '.exe' : '';
const targetName = `agent-browser-${platformKey}${ext}`;
const targetPath = join(binDir, targetName);

if (!existsSync(sourcePath)) {
  console.error(`Error: Native binary not found at ${sourcePath}`);
  console.error('Run "cargo build --release --manifest-path cli/Cargo.toml" first');
  process.exit(1);
}

if (!existsSync(binDir)) {
  mkdirSync(binDir, { recursive: true });
}

// Unlink first so the copy lands on a FRESH inode. Overwriting a code-signed Mach-O in place (same inode)
// leaves macOS serving a stale signature from its per-(dev,inode) cache, and the kernel then SIGKILLs the
// binary on exec ("killed: 9", 0 output) even though `codesign --verify` reports it valid on disk — which
// silently breaks `agent-browser` and any tool that spawns it (e.g. fleetmux's FLEETMUX_AGENT_BROWSER_BIN).
rmSync(targetPath, { force: true });
copyFileSync(sourcePath, targetPath);
console.log(`✓ Copied native binary to ${targetPath}`);
