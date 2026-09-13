export function isTorrent(name: string): boolean {
  return name.toLowerCase().endsWith('.torrent')
}
