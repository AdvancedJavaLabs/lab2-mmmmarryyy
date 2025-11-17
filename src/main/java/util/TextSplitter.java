package util;

import config.AppConfig.ChunkBy;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TextSplitter {
    public static Iterator<String> splitStream(BufferedReader reader, ChunkBy chunkBy, int chunkSize) {
        switch (chunkBy) {
            case PARAGRAPHS:
                return new ParagraphSplitter(reader, chunkSize);
            case SENTENCES:
                return new SentenceSplitter(reader, chunkSize);
            case BYTES:
            default:
                return new ByteSplitter(reader, chunkSize);
        }
    }

    private abstract static class StreamSplitter implements Iterator<String> {
        protected final BufferedReader reader;
        protected final int chunkSize;
        protected String nextChunk;
        protected boolean hasNext = true;

        public StreamSplitter(BufferedReader reader, int chunkSize) {
            this.reader = reader;
            this.chunkSize = chunkSize;
            this.nextChunk = null;
        }

        @Override
        public boolean hasNext() {
            if (nextChunk != null) {
                return true;
            }
            if (!hasNext) {
                return false;
            }
            try {
                nextChunk = readNextChunk();
                if (nextChunk == null) {
                    hasNext = false;
                    return false;
                }
                return true;
            } catch (IOException e) {
                hasNext = false;
                throw new RuntimeException("Error reading from stream", e);
            }
        }

        @Override
        public String next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            String result = nextChunk;
            nextChunk = null;
            return result;
        }

        protected abstract String readNextChunk() throws IOException;
    }

    private static class ParagraphSplitter extends StreamSplitter {
        private final StringBuilder currentChunk = new StringBuilder();
        private int paragraphCount = 0;

        public ParagraphSplitter(BufferedReader reader, int chunkSize) {
            super(reader, chunkSize);
        }

        @Override
        protected String readNextChunk() throws IOException {
            String line;

            while ((line = reader.readLine()) != null) {
                if (line.trim().isEmpty()) {
                    if (!currentChunk.isEmpty()) {
                        paragraphCount++;

                        if (paragraphCount >= chunkSize) {
                            String result = currentChunk.toString().trim();
                            currentChunk.setLength(0);
                            paragraphCount = 0;
                            return result;
                        }
                    }
                } else {
                    if (!currentChunk.isEmpty()) {
                        currentChunk.append(" ");
                    }
                    currentChunk.append(line.trim());
                }
            }

            if (!currentChunk.isEmpty()) {
                String result = currentChunk.toString().trim();
                currentChunk.setLength(0);
                return result;
            }

            return null;
        }
    }

    private static class SentenceSplitter extends StreamSplitter {
        private final StringBuilder currentChunk = new StringBuilder();
        private final StringBuilder buffer = new StringBuilder();
        private int sentenceCount = 0;
        private static final Pattern SENTENCE_PATTERN = Pattern.compile("[.!?]\\s+");

        public SentenceSplitter(BufferedReader reader, int chunkSize) {
            super(reader, chunkSize);
        }

        @Override
        protected String readNextChunk() throws IOException {
            while (sentenceCount < chunkSize) {
                if (buffer.isEmpty()) {
                    String line = reader.readLine();

                    if (line == null) {
                        break;
                    }

                    buffer.append(line.trim());

                    if (!buffer.isEmpty()) {
                        buffer.append(" ");
                    }

                    continue;
                }

                String bufferText = buffer.toString();
                Matcher matcher = SENTENCE_PATTERN.matcher(bufferText);

                if (matcher.find()) {
                    int splitPosition = matcher.end();
                    String sentence = bufferText.substring(0, splitPosition).trim();

                    if (!sentence.isEmpty()) {
                        if (!currentChunk.isEmpty()) {
                            currentChunk.append(" ");
                        }

                        currentChunk.append(sentence);
                        sentenceCount++;
                    }

                    buffer.delete(0, splitPosition);
                } else {
                    String line = reader.readLine();

                    if (line == null) {
                        if (!buffer.isEmpty()) {
                            String finalSentence = buffer.toString().trim();

                            if (!finalSentence.isEmpty()) {
                                if (!currentChunk.isEmpty()) {
                                    currentChunk.append(". ");
                                }

                                currentChunk.append(finalSentence);
                                sentenceCount++;
                            }

                            buffer.setLength(0);
                        }

                        break;
                    }

                    buffer.append(line.trim());

                    if (!buffer.isEmpty()) {
                        buffer.append(" ");
                    }
                }
            }

            if (sentenceCount >= chunkSize || (buffer.isEmpty() && !currentChunk.isEmpty())) {
                String result = currentChunk.toString();
                currentChunk.setLength(0);
                sentenceCount = 0;
                return result;
            }

            return null;
        }
    }

    private static class ByteSplitter extends StreamSplitter {
        private final char[] buffer = new char[8192];
        private int bufferPosition = 0;
        private int bufferSize = 0;
        private boolean eof = false;

        public ByteSplitter(BufferedReader reader, int chunkSize) {
            super(reader, chunkSize);
        }

        @Override
        protected String readNextChunk() throws IOException {
            if (eof && bufferPosition >= bufferSize) {
                return null;
            }

            StringBuilder chunk = new StringBuilder();
            int remaining = chunkSize;

            while (remaining > 0) {
                if (bufferPosition >= bufferSize) { // то есть буфер пуст
                    bufferSize = reader.read(buffer);
                    bufferPosition = 0;

                    if (bufferSize == -1) {
                        eof = true;
                        break;
                    }
                }

                int charsToRead = Math.min(remaining, bufferSize - bufferPosition);
                chunk.append(buffer, bufferPosition, charsToRead);
                bufferPosition += charsToRead;
                remaining -= charsToRead;
            }

            return !chunk.isEmpty() ? chunk.toString() : null;
        }
    }
}