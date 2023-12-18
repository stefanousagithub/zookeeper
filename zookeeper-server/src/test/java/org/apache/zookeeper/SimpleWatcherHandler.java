package org.apache.zookeeper;

import org.junit.Assert;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class SimpleWatcherHandler implements  Watcher{

    //Classe di supporto per il test MyZookeeperSetDataTest


        private LinkedBlockingQueue<WatchedEvent> events =
                new LinkedBlockingQueue<WatchedEvent>();
        private CountDownLatch latch;       //controllo sync: aspetta che altre
        //operazioni siano terminate

        public SimpleWatcherHandler(CountDownLatch latch) {
            this.latch = latch;
        }

        public void process(WatchedEvent event) {
            //client è connesso al server

            //poi prova a commentare questo pezzo
            if (event.getState() == Event.KeeperState.SyncConnected) {
                if (latch != null) {
                    latch.countDown();  //decrementa il count del latch, finche
                                        //stato di wait non termina
                }
            }

            if (event.getType() == Event.EventType.None) {
                return;
            }
            try {
                //aggiungi evento che triggera watcher
                events.put(event);
            } catch (InterruptedException e) {
                Assert.assertTrue("interruption unexpected", false);
            }
        }

        //verifica che watcher contiene gli eventi presenti in expected
        public boolean verify(List<Event.EventType> expected) throws InterruptedException{
            WatchedEvent event;
            int count = 0;
            boolean res = false;
            while (count < expected.size()
                    && (event = events.poll(30, TimeUnit.SECONDS)) != null)
            {

                if (expected.get(count).equals(event.getType())){
                    count++;
                }

            }
            if (expected.size() == count){
                res = true;
            }

            events.clear();
            return res;
        }
    }

