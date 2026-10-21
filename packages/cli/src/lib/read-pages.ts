export interface ReadPageEnvelope<T> {
  items: T[];
  next_cursor: string | null;
  release_token: string | null;
}

/** Read a frozen page sequence serially and always release its retained sample. */
export async function readAllPages<T>(
  fetchPage: (cursor?: string) => Promise<ReadPageEnvelope<T>>,
  release: (token: string) => Promise<{ released: boolean }>,
  onPage: (page: ReadPageEnvelope<T>) => void | Promise<void>,
  isInterrupted: () => boolean = () => false,
): Promise<void> {
  let cursor: string | undefined;
  let releaseToken: string | null = null;
  const seenCursors = new Set<string>();
  try {
    while (true) {
      const page = await fetchPage(cursor);
      if (page.release_token) releaseToken = page.release_token;
      await onPage(page);
      if (isInterrupted()) throw new Error("V2 read interrupted");
      if (page.next_cursor === null) break;
      if (seenCursors.has(page.next_cursor)) throw new Error("V2 read returned a repeated cursor");
      seenCursors.add(page.next_cursor);
      cursor = page.next_cursor;
    }
  } finally {
    if (releaseToken !== null) await release(releaseToken);
  }
}
