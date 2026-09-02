import type { Metadata } from 'next';
import Script from 'next/script';
import { Providers } from './providers';
import './globals.css';

export const metadata: Metadata = {
  title: '@agentic-kit/core — example',
  description: 'Queue-dispatched agent runs with SSE sync, HITL approval, and subagents',
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body>
        <Providers>{children}</Providers>
        <Script id="move-devtools-indicator" strategy="afterInteractive">
          {`
            (function () {
              function apply() {
                var el = document.getElementById('devtools-indicator');
                if (!el) return false;
                el.style.bottom = '100px';
                return true;
              }
              if (apply()) return;
              var observer = new MutationObserver(function () {
                if (apply()) observer.disconnect();
              });
              observer.observe(document.documentElement, { childList: true, subtree: true });
            })();
          `}
        </Script>
      </body>
    </html>
  );
}
