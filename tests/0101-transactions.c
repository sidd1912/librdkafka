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


/**
 * @brief Consumes \p cnt messages and returns them in the provided array
 *        which must be pre-allocated.
 */
static void consume_messages (rd_kafka_t *c,
                              rd_kafka_message_t **msgs, int msgcnt) {
        int i = 0;
        while (i < msgcnt) {
                msgs[i] = rd_kafka_consumer_poll(c, 1000);
                if (!msgs[i])
                        continue;

                if (msgs[i]->err) {
                        TEST_SAY("%s consumer error: %s\n",
                                 rd_kafka_name(c),
                                 rd_kafka_message_errstr(msgs[i]));
                        rd_kafka_message_destroy(msgs[i]);
                        continue;
                }

                i++;
        }
}


/**
 * @brief Test a transactional consumer + transactional producer combo,
 *        mimicing a streams job.
 *
 * One input topic produced to by transactional producer 1,
 * consumed by transactional consumer 1, which forwards messages
 * to transactional producer 2 that writes messages to output topic,
 * which is consumed and verified by transactional consumer 2.
 *
 * Every 3rd transaction is aborted.
 */
void do_test_consumer_producer_txn (void) {
        const char *input_topic =
                test_mk_topic_name("0101-transactions-input", 1);
        const char *output_topic =
                test_mk_topic_name("0101-transactions-output", 1);
        const char *c1_groupid = input_topic;
        const char *c2_groupid = output_topic;
        rd_kafka_t *p1, *p2, *c1, *c2;
        rd_kafka_conf_t *conf, *tmpconf, *c1_conf;
        uint64_t testid;
        int txncnt = 10;
        int msgcnt = txncnt * 10;
        int committed_msgcnt = 0;
        char errstr[512];
        test_msgver_t expect_mv, actual_mv;

        TEST_SAY(_C_BLU "[ Transactional test with %d transactions ]\n",
                 txncnt);

        test_conf_init(&conf, NULL, 30);

        testid = test_id_generate();

        /*
         *
         * Producer 1
         *     |
         *     v
         * input topic
         *     |
         *     v
         * Consumer 1    }
         *     |         } transactional streams job
         *     v         }
         * Producer 2    }
         *     |
         *     v
         * output tpic
         *     |
         *     v
         * Consumer 2
         */


        /* Create Producer 1 and seed input topic */
        tmpconf = rd_kafka_conf_dup(conf);
        test_conf_set(tmpconf, "transactional.id", input_topic);
        rd_kafka_conf_set_dr_msg_cb(tmpconf, test_dr_msg_cb);
        p1 = test_create_handle(RD_KAFKA_PRODUCER, tmpconf);

        /* Create input and output topics */
        test_create_topic(p1, input_topic, 4, 3);
        test_create_topic(p1, output_topic, 4, 3);

        /* Seed input topic with messages */
        TEST_CALL__(rd_kafka_init_transactions(p1, errstr, sizeof(errstr)));
        TEST_CALL__(rd_kafka_begin_transaction(p1, errstr, sizeof(errstr)));
        test_produce_msgs2(p1, input_topic, testid, RD_KAFKA_PARTITION_UA,
                           0, msgcnt, NULL, 0);
        TEST_CALL__(rd_kafka_commit_transaction(p1, errstr, sizeof(errstr)));
        rd_kafka_destroy(p1);

        /* Create Consumer 1 */
        tmpconf = rd_kafka_conf_dup(conf);
        test_conf_set(tmpconf, "isolation.level", "read_committed");
        test_conf_set(tmpconf, "auto.offset.reset", "earliest");
        test_conf_set(tmpconf, "enable.auto.commit", "false");
        c1_conf = rd_kafka_conf_dup(tmpconf);
        c1 = test_create_consumer(c1_groupid, NULL, tmpconf, NULL);
        test_consumer_subscribe(c1, input_topic);

        /* Create Producer 2 */
        tmpconf = rd_kafka_conf_dup(conf);
        test_conf_set(tmpconf, "transactional.id", output_topic);
        rd_kafka_conf_set_dr_msg_cb(tmpconf, test_dr_msg_cb);
        p2 = test_create_handle(RD_KAFKA_PRODUCER, tmpconf);
        TEST_CALL__(rd_kafka_init_transactions(p2, errstr, sizeof(errstr)));

        /* Create Consumer 2 */
        tmpconf = rd_kafka_conf_dup(conf);
        test_conf_set(tmpconf, "isolation.level", "read_committed");
        test_conf_set(tmpconf, "auto.offset.reset", "earliest");
        c2 = test_create_consumer(c2_groupid, NULL, tmpconf, NULL);
        test_consumer_subscribe(c1, output_topic);

        rd_kafka_conf_destroy(conf);

        /* Keep track of what messages to expect on the output topic */
        test_msgver_init(&expect_mv, testid);

        while (txncnt-- > 0) {
                int msgcnt = 10 * (1 + (txncnt % 3));
                rd_kafka_message_t *msgs[msgcnt];
                int i;
                rd_bool_t do_abort = !(txncnt % 3);
                rd_kafka_topic_partition_list_t *offsets;
                rd_kafka_resp_err_t err;

                consume_messages(c1, msgs, msgcnt);

                TEST_CALL__(rd_kafka_begin_transaction(p2,
                                                       errstr,
                                                       sizeof(errstr)));
                for (i = 0 ; i < msgcnt ; i++) {
                        rd_kafka_message_t *msg = msgs[i];

                        if (!do_abort) {
                                test_msgver_add_msg(&expect_mv, msg);
                                committed_msgcnt++;
                        }

                        err = rd_kafka_producev(p2,
                                                RD_KAFKA_V_TOPIC(output_topic),
                                                RD_KAFKA_V_KEY(msg->key,
                                                               msg->key_len),
                                                RD_KAFKA_V_VALUE(msg->payload,
                                                                 msg->len),
                                                RD_KAFKA_V_MSGFLAGS(
                                                        RD_KAFKA_MSG_F_COPY),
                                                RD_KAFKA_V_END);
                        TEST_ASSERT(!err, "produce failed: %s",
                                    rd_kafka_err2str(err));

                        rd_kafka_poll(p2, 0);
                }

                err = rd_kafka_assignment(c1, &offsets);
                TEST_ASSERT(!err, "failed to get consumer assignment: %s",
                            rd_kafka_err2str(err));

                err = rd_kafka_position(c1, offsets);
                TEST_ASSERT(!err, "failed to get consumer position: %s",
                            rd_kafka_err2str(err));

                TEST_CALL__(
                        rd_kafka_send_offsets_to_transaction(
                                p2, offsets, c1_groupid,
                                errstr, sizeof(errstr)));

                rd_kafka_topic_partition_list_destroy(offsets);


                if (do_abort)
                        TEST_CALL__(rd_kafka_abort_transaction(
                                            p2, errstr, sizeof(errstr)));
                else
                        TEST_CALL__(rd_kafka_commit_transaction(
                                            p2, errstr, sizeof(errstr)));


                if (txncnt == 4) {
                        /* Recreate the consumer to pick up
                         * on the committed offset. */
                        TEST_SAY("Recreating consumer 1\n");
                        rd_kafka_consumer_close(c1);
                        rd_kafka_destroy(c1);

                        c1 = test_create_consumer(c1_groupid, NULL, c1_conf,
                                                  NULL);
                        test_consumer_subscribe(c1, input_topic);
                }
        }

        test_msgver_init(&actual_mv, testid);

        test_consumer_poll("Verify output topic", c2, testid,
                           -1, 0, committed_msgcnt, &actual_mv);

        test_msgver_verify_compare("Verify output topic",
                                   &actual_mv, &expect_mv,
                                   TEST_MSGVER_ALL);

        test_msgver_clear(&actual_mv);
        test_msgver_clear(&expect_mv);

        rd_kafka_consumer_close(c1);
        rd_kafka_consumer_close(c2);
        rd_kafka_destroy(c1);
        rd_kafka_destroy(c2);
        rd_kafka_destroy(p2);
}



int main_0101_transactions (int argc, char **argv) {
        const char *topic = test_mk_topic_name("0101_transactions", 1);

        do_test_consumer_producer_txn();

        do_test_basic_producer_txn(topic);

        if (test_quick)
                return 0;

        return 0;
}
