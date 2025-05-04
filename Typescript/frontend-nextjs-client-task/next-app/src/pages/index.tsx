import Head from "next/head";
import Image from "next/image";
import styles from "../styles/Home.module.css";
import {JsonClient} from "../components/JsonClient";

export default function Home() {
  return (
    <div className={styles.container}>
      <Head>
        <title>Web client for the tool built</title>
        <link rel="icon" href="/favicon.ico" />
      </Head>

      <main className={styles.main}>
        <h1 className={styles.title}>
          Trading Engine Client
        </h1>

        <hr className="m-4"/>

          <JsonClient  />


      </main>

      <footer className={styles.footer}>
       Reza Mir
      </footer>
    </div>
  );
}
