import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "Aviation & Weather Observability Lakehouse",
  description: "End-to-end Medallion Data Engineering Pipeline with Databricks, Delta Lake, Neon Postgres, and Vercel",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <body className="antialiased min-h-screen bg-slate-950 text-slate-100">{children}</body>
    </html>
  );
}
