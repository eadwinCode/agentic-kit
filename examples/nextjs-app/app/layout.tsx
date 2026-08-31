import type { Metadata } from 'next';
import './globals.css';

export const metadata: Metadata = {
  title: '@agent/core — example',
  description: 'Queue-dispatched agent runs with SSE sync, HITL approval, and subagents',
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
