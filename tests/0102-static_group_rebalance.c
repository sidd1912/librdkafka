/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2012-2019, Magnus Edenhill
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

/* Typical include path would be <librdkafka/rdkafka.h>, but this program
 * is built from within the librdkafka source tree and thus differs. */
#include "rdkafka.h" /* for Kafka driver */

/**
 * KafkaConsumer balanced group with multithreading tests
 *
 * Runs a consumer subscribing to a topic with multiple partitions and farms
 * consuming of each partition to a separate thread.
 */

typedef struct _consumer_s {
        rd_kafka_t *rk;
        int assign_cnt;
        int max_rebalance_cnt;
} _consumer_t;

rd_list_t worker_list;

static void do_consume (_consumer_t *cons, int timeout_s) {
        rd_kafka_message_t *rkm;

        rkm = rd_kafka_consumer_poll(cons->rk, 100+(timeout_s*1000));
        if (!rkm)
                return;

        if(rkm->err)
                TEST_SAY(
                    "%s consumer error: %s",
                    rd_kafka_name(cons->rk),
                    rd_kafka_message_errstr(rkm));

        rd_kafka_message_destroy(rkm);

        if (timeout_s > 0) {
                TEST_SAY("%s: simulate processing by sleeping for %ds\n",
                         rd_kafka_name(cons->rk), timeout_s);
                rd_sleep(timeout_s);
        }
}

/* Serve op queue until we have an assignment */
static void await_assignment (_consumer_t *rk_c) {
        TEST_SAY("Waiting for partition assignment\n");
        while (1) {
                rd_kafka_topic_partition_list_t *parts = NULL;

                do_consume(rk_c, 1/*1s*/);

                if (rd_kafka_assignment(rk_c->rk, &parts) !=
                    RD_KAFKA_RESP_ERR_NO_ERROR ||
                    !parts || parts->cnt == 0) {
                        if (parts)
                                rd_kafka_topic_partition_list_destroy(parts);
                        continue;
                }

                TEST_SAY("%s got assignment of %d partition(s)\n",
                         rd_kafka_name(rk_c->rk), parts->cnt);
                rd_kafka_topic_partition_list_destroy(parts);
                break;
        }
}

static void rebalance_cb (rd_kafka_t *rk,
                          rd_kafka_resp_err_t err,
                          rd_kafka_topic_partition_list_t *parts,
                          void *opaque) {

        _consumer_t *rk_c = opaque;

        TEST_SAY("%s rebalance #%d/%d: %s: %d partition(s)\n",
                 rd_kafka_name(rk),
                 rk_c->assign_cnt, rk_c->max_rebalance_cnt,
                 rd_kafka_err2name(err),
                 parts->cnt);

        switch (err)
        {
        case RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
                rk_c->assign_cnt++;

                rd_kafka_assign(rk, parts);

                TEST_ASSERT(rk_c->assign_cnt <= rk_c->max_rebalance_cnt,
                            "%s rebalanced %d times, max was %d",
                            rd_kafka_name(rk),
                            rk_c->assign_cnt, rk_c->max_rebalance_cnt);
                break;  

        case RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
                rd_kafka_assign(rk, NULL);
                break;

        default:
                TEST_FAIL("rebalance failed: %s", rd_kafka_err2str(err));
                break;
        }

}

#define _CONSUMER_CNT 2
int main_0102_static_group_rebalance (int argc, char **argv) {
        rd_kafka_conf_t *conf;

        const int msgcnt = 100;
        const char *topic = test_mk_topic_name("0101_static_group_rebalance", 1);
        _consumer_t c[_CONSUMER_CNT] = RD_ZERO_INIT;

        test_conf_init(&conf, NULL, 60);

        test_conf_set(conf, "session.timeout.ms", "1000");
        test_conf_set(conf, "auto.offset.reset", "earliest");
        test_conf_set(conf, "topic.metadata.refresh.interval.ms", "500");

        rd_kafka_conf_set_opaque(conf, &c[0]);
        test_conf_set(conf, "group.instance.id", "consumer1");
        c[0].rk = test_create_consumer("static", &rebalance_cb,
                                       rd_kafka_conf_dup(conf), NULL);

        rd_kafka_conf_set_opaque(conf, &c[1]);
        test_conf_set(conf, "group.instance.id", "consumer2");
        c[1].rk = test_create_consumer("static", &rebalance_cb,
                                       rd_kafka_conf_dup(conf), NULL);

        c[0].max_rebalance_cnt = 1;
        c[1].max_rebalance_cnt = 1;

        test_consumer_subscribe(c[0].rk, rd_strdup(tsprintf("^%s.*", topic)));
        test_consumer_subscribe(c[1].rk, rd_strdup(tsprintf("^%s.*", topic)));

        test_create_topic(NULL, topic, 3, 1);
        test_produce_msgs_easy(topic, test_id_generate(), 0, msgcnt);

        TEST_SAY("Waiting for partition assignment\n");
        await_assignment(&c[0]);
        await_assignment(&c[1]);

        TEST_SAY("Bouncing c[1] instance.\n");
        test_consumer_close(c[1].rk);
        rd_kafka_destroy(c[1].rk);

        /** 
         * Removeing and adding a new member with the same group instance id 
         * should not prompt a rebalance for the original remaining member.
         */
        c[1].max_rebalance_cnt++;
        c[1].rk = test_create_consumer("static", &rebalance_cb,
                                       conf, NULL);
        test_consumer_subscribe(c[1].rk, rd_strdup(tsprintf("^%s.*", topic)));

        /* Wait for c[1] to rejoin, serve c[0] ops */
        await_assignment(&c[1]);
        do_consume(&c[0], 5/*5s*/);

        TEST_SAY("Creating a new topic.\n");
        /* Expanding the subscription forces a rebalance */
        c[0].max_rebalance_cnt++;
        c[1].max_rebalance_cnt++;

        /* The topic prefix uses the test id which is "random" */
        test_create_topic(c[0].rk, rd_strdup(tsprintf("%snew", topic)), 1, 1);
        do_consume(&c[0], 5/*5s*/);
        do_consume(&c[1], 5/*5s*/);
        await_assignment(&c[0]);

        /* Wait until session.timeout.ms is exceeded to force a rebalance. */
        TEST_SAY("Closing c[1], waiting for static instance to be evicted.\n");
        c[0].max_rebalance_cnt++;

        test_consumer_close(c[1].rk);
        rd_kafka_destroy(c[1].rk);

        /* 2x heartbeat interval to give time for c[0] to recognize rebalance */
        rd_sleep(6);
        do_consume(&c[0], 2/*1s*/);

        TEST_ASSERT(c[0].assign_cnt == c[0].max_rebalance_cnt,
                    "c[0] rebalanced %d times, expected %d",
                    c[0].assign_cnt, c[0].max_rebalance_cnt);

        TEST_SAY("Closing remaining consumers\n");
        test_consumer_close(c[0].rk);
        rd_kafka_destroy(c[0].rk);

        return 0;
}