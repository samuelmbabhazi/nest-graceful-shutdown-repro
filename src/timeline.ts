const startedAt = Date.now();
const lines: string[] = [];

export const seconds = () => (Date.now() - startedAt) / 1000;

export const sleep = (ms: number) =>
  new Promise<void>(resolve => setTimeout(resolve, ms));

export function mark(message: string) {
  lines.push(`${seconds().toFixed(1).padStart(5)}s  ${message}`);
}

export function print(title: string) {
  console.log(`=== ${title}`);
  console.log(lines.join('\n'));
}
