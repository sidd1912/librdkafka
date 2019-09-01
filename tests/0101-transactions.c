/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2019, Magnus Edenhill
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include "test.h"

#include "rdkafka.h"

/**
 * @name Transaction test
 *
 */


/**
 * @brief Basic producer transaction testing without consumed input
 *        (only consumed output for verification).
 *        e.g., no consumer offsets to commit with transaction.
 */
static void do_test_basic_producer_txn (const char *topic) {
        const int partition_cnt = 4;
#define _TXNCNT 4
        struct {
                const char *desc;
                uint64_t testid;
                int msgcnt;
                rd_bool_t abort;
                rd_bool_t sync;
        } txn[_TXNCNT] = {
                { "Commit transaction, sync producing",
                  0, 100, rd_false, rd_true },
                { "Commit transaction, async producing",
                  0, 1000, rd_false, rd_false },
                { "Abort transaction, sync producing",
                  0, 500, rd_true, rd_true },
                { "Abort transaction, async producing",
                  0, 5000, rd_true, rd_false },
        };
        rd_kafka_t *p, *c;
        rd_kafka_conf_t *conf, *p_conf, *c_conf;
        int i;
        char errstr[256];

        test_conf_init(&conf, NULL, 30);

        /* Create producer */
        p_conf = rd_kafka_conf_dup(conf);
        rd_kafka_conf_set_dr_msg_cb(p_conf, test_dr_msg_cb);
        test_conf_set(p_conf, "transactional.id", topic);
        p = test_create_handle(RD_KAFKA_PRODUCER, p_conf);

        // FIXME: add testing were the txn id is reused (and thus fails)

        /* Create topic */
        test_create_topic(p, topic, partition_cnt, 3);

        /* Create consumer */
        c_conf = conf;
        test_conf_set(c_conf, "isolation.level", "read_committed");
        c = test_create_consumer(topic, NULL, c_conf, NULL);

        /* Subscribe to topic */
        test_consumer_subscribe(c, topic);

        /* Wait for assignment to make sure consumer is fetching messages
         * below, so we can use the poll_no_msgs() timeout to
         * determine that messages were indeed aborted. */
        test_consumer_wait_assignment(c);

        /* Init transactions */
        TEST_CALL__(rd_kafka_init_transactions(p, errstr, sizeof(errstr)));

        for (i = 0 ; i < _TXNCNT ; i++) {
                int wait_msgcnt = 0;

                TEST_SAY(_C_BLU "txn[%d]: Begin transaction: %s\n" _C_CLR,
                         i, txn[i].desc);

                /* Begin a transaction */
                TEST_CALL__(rd_kafka_begin_transaction(p,
                                                       errstr,
                                                       sizeof(errstr)));

                /* If the transaction is aborted it is okay if
                 * messages fail producing, since they'll be
                 * purged from queues. */
                test_curr->ignore_dr_err = txn[i].abort;

                /* Produce messages */
                txn[i].testid = test_id_generate();
                TEST_SAY("txn[%d]: Produce %d messages %ssynchronously "
                         "with testid %"PRIu64"\n",
                         i, txn[i].msgcnt,
                         txn[i].sync ? "" : "a",
                         txn[i].testid);

                if (txn[i].sync)
                        test_produce_msgs2(p, topic, txn[i].testid,
                                           RD_KAFKA_PARTITION_UA, 0,
                                           txn[i].msgcnt, NULL, 0);
                else
                        test_produce_msgs2_nowait(p, topic, txn[i].testid,
                                                  RD_KAFKA_PARTITION_UA, 0,
                                                  txn[i].msgcnt, NULL, 0,
                                                  &wait_msgcnt);

                /* Abort or commit transaction */
                TEST_SAY("txn[%d]: %s" _C_CLR " transaction\n",
                         i, txn[i].abort ? _C_RED "Abort" : _C_GRN "Commit");
                if (txn[i].abort)
                        TEST_CALL__(rd_kafka_abort_transaction(
                                            p, errstr, sizeof(errstr)));
                else
                        TEST_CALL__(rd_kafka_commit_transaction(
                                            p, errstr, sizeof(errstr)));

                if (!txn[i].sync)
                        /* Wait for delivery reports */
                        test_wait_delivery(p, &wait_msgcnt);

                /* Consume messages */
                if (txn[i].abort)
                        test_consumer_poll_no_msgs(txn[i].desc, c,
                                                   txn[i].testid, 3000);
                else
                        test_consumer_poll(txn[i].desc, c,
                                           txn[i].testid, partition_cnt, 0,
                                           txn[i].msgcnt, NULL);

                TEST_SAY(_C_GRN "txn[%d]: Finished successfully: %s\n" _C_CLR,
                         i, txn[i].desc);
        }

        rd_kafka_destroy(p);

        test_consumer_close(c);
        rd_kafka_destroy(c);
}

int main_0101_transactions (int argc, char **argv) {
        const char *topic = test_mk_topic_name("0101_transactions", 1);

        do_test_basic_producer_txn(topic);

        if (test_quick)
                return 0;

        return 0;
}
