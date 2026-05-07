import type { Metadata } from 'next';
import './globals.css';
import styles from './layout.module.css';
import Sidebar from '../components/Sidebar';
import Header from '../components/Header';

export const metadata: Metadata = {
  title: 'VesselWatch',
  description: 'Modern Government Oil Spill Detection Dashboard',
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en" suppressHydrationWarning>
      <head>
        <link rel="preconnect" href="https://fonts.googleapis.com" />
        <link rel="preconnect" href="https://fonts.gstatic.com" crossOrigin="anonymous" />
        <link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@500;600&family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet" />
      </head>
      <body suppressHydrationWarning>
        <div className={styles.appContainer}>
          <Sidebar />
          <div className={styles.mainWrapper}>
            <Header />
            <main className={styles.mainContent}>
              {children}
            </main>
          </div>
        </div>
      </body>
    </html>
  );
}
