'use client';

import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { useState } from "react";

export function ReactQueryProvider({ children }: { children: React.ReactNode }) {
  const [queryClient] = useState(
    () =>
      new QueryClient({
        defaultOptions: {
          queries: {
            // Data is considered fresh for 60 s by default
            staleTime: 60_000,
            // Keep unused cache for 5 min (survives page switches)
            gcTime: 5 * 60_000,
            // Retry failed requests up to 2 times with exponential backoff
            retry: 2,
            retryDelay: (n) => Math.min(1000 * 2 ** n, 30_000),
          },
        },
      })
  );

  return (
    <QueryClientProvider client={queryClient}>
      {children}
    </QueryClientProvider>
  );
}
