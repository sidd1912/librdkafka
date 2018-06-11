/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2018 Magnus Edenhill
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

/**
 * Questions:
 *
 * - initTransactions(): there are two asynchronous operations, one is
 *   is the coordinator lookup and the other is the InitProducerIdRequest.
 *   Both may need to be retried.
 *   The Java Producer uses `max.block.ms` to limit the maximum amount this
 *   make take, but I'd rather not add that configuration property since
 *   it mixes up APIs with user configuration. So, the alternatives are:
 *    a) a timeout_ms argument, b) truly asynchronous with queue_t
 *
 *
 *
 */

#include "rd.h"
#include "rdkafka_int.h"


static const char *rd_kafka_txn_state_names[] = {
        "UNINITIALIZED",
        "INITIALIZING",
        "READY",
        "IN_TRANSACTION",
        "COMMITTING_TRANSACTION",
        "ABORTING_TRANSACTION",
        "ABORTABLE_ERROR",
        "FATAL_ERROR"
};


static RD_INLINE rd_kafka_resp_err_t
rd_kafka_ensure_transactional (const rd_kafka_t *rk,
                               const char *errstr, size_t errstr_size) {
        if (unlikely(!rk->rk_txn.transactionalId))
                return RD_KAFKA_RESP_ERR__STATE;
        return RD_KAFKA_RESP_ERR_NO_ERROR;
}

/**
 * @brief Ensure initTransactions() has been called.
 * @locks rd_kafka_*lock() MUST be held
 */
static RD_INLINE rd_kafka_resp_err_t
rd_kafka_ensure_txn_initialized (const rd_kafka_t *rk,
                                 const char *errstr, size_t errstr_size) {
        if (unlikely(!rk->rk_txn.PidEpoch.pid))
                return RD_KAFKA_RESP_ERR__STATE;
        return RD_KAFKA_RESP_ERR_NO_ERROR;
}


/**
 * @brief Ensure the transaction is in an error state
 * @locks rd_kafka_*lock() MUST be held
 */
static RD_INLINE rd_kafka_resp_err_t
rd_kafka_ensure_txn_no_error (const rd_kafka_t *rk,
                              const char *errstr, size_t errstr_size) {
        switch (rk->rk_txn.state)
        {
        case RD_KAFKA_TXN_STATE_ABORTABLE_ERROR:
        case RD_KAFKA_TXN_STATE_FATAL_ERROR:
                return RD_KAFKA_RESP_ERR__TXN_ERR_STATE;
        default:
                return RD_KAFKA_RESP_ERR_NO_ERROR;
        }
}

/**
 * @returns true if state transition is valid, else false.
 */
static rd_bool
rd_kafka_txn_state_transition_valid (rd_kafka_txn_state_t old_state,
                                     rd_kafka_txn_state_t new_state) {
        switch (new_state) {
        case RD_KAFKA_TXN_STATE_INITIALIZING:
                return old_state == RD_KAFKA_TXN_STATE_UNINITIALIZED;
        case RD_KAFKA_TXN_STATE_READY:
                return old_state == RD_KAFKA_TXN_STATE_INITIALIZING ||
                        old_state == RD_KAFKA_TXN_STATE_COMMITTING_TRANSACTION ||
                        old_state == RD_KAFKA_TXN_STATE_ABORTING_TRANSACTION;
        case RD_KAFKA_TXN_STATE_IN_TRANSACTION:
                return old_state == RD_KAFKA_TXN_STATE_READY;
        case RD_KAFKA_TXN_STATE_COMMITTING_TRANSACTION:
                return old_state == RD_KAFKA_TXN_STATE_IN_TRANSACTION;
        case RD_KAFKA_TXN_STATE_ABORTING_TRANSACTION:
                return old_state == RD_KAFKA_TXN_STATE_IN_TRANSACTION ||
                        old_state == RD_KAFKA_TXN_STATE_ABORTABLE_ERROR;
        case RD_KAFKA_TXN_STATE_ABORTABLE_ERROR:
                return old_state == RD_KAFKA_TXN_STATE_IN_TRANSACTION ||
                        old_state == RD_KAFKA_TXN_STATE_COMMITTING_TRANSACTION ||
                        old_state == RD_KAFKA_TXN_STATE_ABORTABLE_ERROR;
        case RD_KAFKA_TXN_STATE_FATAL_ERROR:
        default:
                /* We can transition to FATAL_ERROR unconditionally.
                 * FATAL_ERROR is never a valid starting state for any transition.
                 * So the only option is to destroy the producer or do purely
                 * non-transactional requests. */
                return rd_true;
        }
}


/**
 * @brief Transition the transaction state to \p new_state.
 *
 * @returns 0 on success or an error code if the state transition
 *          was invalid.
 *
 * @locality FIXME:any?
 * @locks rd_kafka_wrlock MUST be held
 */
static rd_kafka_resp_err_t
rd_kafka_txn_set_state (rd_kafka_t *rk, rd_kafka_txn_state_t new_state) {
        if (!rd_kafka_txn_state_transition_valid(rk->rk_txn.state, new_state)) {
                rd_kafka_log(rk, LOG_ERR, "TXNSTATE",
                             "Invalid state transition attempted: %s -> %s",
                             rd_kafka_txn_state_names[rk->rk_txn.state],
                             rd_kafka_txn_state_names[new_state]);
                return RD_KAFKA_RESP_ERR__STATE;
        }

        rd_kafka_dbg(rk, TXN, "TXNSTATE",
                     "Transaction state change %s -> %s",
                     rd_kafka_txn_state_names[rk->rk_txn.state],
                     rd_kafka_txn_state_names[new_state]);

        /* FIXME: Java TransactionManager has last_error, do we need it? */

        rk->rk_txn.state = new_state;

        return RD_KAFKA_RESP_ERR_NO_ERROR;
}


/**
 * @brief Set ProducerId and Epoch
 *
 * @locks rd_kafka_wrlock() MUST be held
 */
static void
rd_kafka_txn_set_PidEpoch (rk_kafka_t *rk, const rd_kafka_PidEpoch PidEpoch) {
        rk->rk_txn.PidEpoch = PidEpoch;
        rd_kafka_dbg(rk, TXN, "TXNPID",
                     "ProducerId set to %"PRId32" with epoch %hd",
                     rk->rk_txn.PidEpoch.pid, rk->rk_txn.PidEpoch.epoch);
}

static void rd_kafka_txn_next_seq_clear (rd_kafka_t *rk) {
        // FIXME
}

/**
 * Needs to be called before any other methods when the transactional.id is set in the configuration.
 *
 * This method does the following:
 *   1. Ensures any transactions initiated by previous instances of the producer with the same
 *      transactional.id are completed. If the previous instance had failed with a transaction in
 *      progress, it will be aborted. If the last transaction had begun completion,
 *      but not yet finished, this method awaits its completion.
 *   2. Gets the internal producer id and epoch, used in all future transactional
 *      messages issued by the producer.
 *
 * Note that this method will raise {@link TimeoutException} if the transactional state cannot
 * be initialized before expiration of {@code max.block.ms}. Additionally, it will raise {@link InterruptException}
 * if interrupted. It is safe to retry in either case, but once the transactional state has been successfully
 * initialized, this method should no longer be used.
 *
 * @throws IllegalStateException if no transactional.id has been configured
 * @throws org.apache.kafka.common.errors.UnsupportedVersionException fatal error indicating the broker
 *         does not support transactions (i.e. if its version is lower than 0.11.0.0)
 * @throws org.apache.kafka.common.errors.AuthorizationException fatal error indicating that the configured
 *         transactional.id is not authorized. See the exception for more details
 * @throws KafkaException if the producer has encountered a previous fatal error or for any other unexpected error
 * @throws TimeoutException if the time taken for initialize the transaction has surpassed <code>max.block.ms</code>.
 * @throws InterruptException if the thread is interrupted while blocked

 *
 * @FIXME use max.block.ms, timeout_ms, or a queue?
 *
 * @returns RD_KAFKA_RESP_ERR__STATE If no transactional.id has been configured.
 *
 * @locality any thread
 * @locks none
 */

rd_kafka_resp_err_t
rd_kafka_initTransactions (rd_kafka_t *rk,
                           const char *errstr, size_t errstr_size) {
        rd_kafka_resp_err_t err;
        rd_kafka_broker_t *rkb;
        rd_kafka_q_t *rkq;
        rd_kafka_op_t *rko;
        rd_kafka_PidEpoch PidEpoch = RD_ZERO_INIT;

        if ((err = rd_kafka_ensure_transactional(rk, errstr, errstr_size)))
                return err;

        rd_kafka_wrlock(rk);
        if ((err = rd_kafka_txn_set_state(rk, RD_KAFKA_TXN_STATE_INITIALIZING,
                                          errstr, errstr_size))) {
                rd_kafka_wrunlock(rk);
                return err;
        }

        rd_kafka_txn_set_ProducerIdEpoch(rk, NULL);

        rd_kafka_txn_next_seq_clear(rk);


        rkb = rd_kafka_txn_get_coord(rk /* FIXME: timeout? */);
        if (!rkb) {
                rd_kafka_txn_set_state(rk, 0 /* FIXME: what state? */);
                rd_kafka_wrunlock(rk);
                return RD_KAFKA_RESP_ERR__WAIT_COORD;
        }

        rd_kafka_wrunlock(rk);

        rkq = rd_kafka_q_new(rk);

        err = rd_kafka_InitProducerIdRequest(rkb, rk->rk_conf.txn.TransactionalId,
                                             RD_KAFKA_REPLYQ(rkq, 0));
        if (err) {
                /* FIXME: what next state? */
                rd_kafka_q_destroy_owner(rkq);
                rd_snprintf(errstr, errstr_size, "FIXME");
                return err;
        }

        rko = rd_kafka_q_pop(rkq, rd_timeout_remains(abs_timeout), 0);

        rd_kafka_q_destroy_owner(rkq);

        if (!rko)
                return RD_KAFKA_RESP_ERR__TIMED_OUT;

        err = rd_kafka_InitProducerId_parse(rk, rko->rko_err, &PidEpoch,
                                            rko->rko_u.xbuf.rkbuf);
        rd_kafka_op_destroy(rko);

        switch (err)
        {
        case RD_KAFKA_RESP_ERR_NO_ERROR:
                rd_kafka_wrlock(rk);
                rd_kafka_txn_set_PidEpoch(rk, PidEpoch);
                err = rd_kafka_txn_set_state(rk, RD_KAFKA_TXN_STATE_READY,
                                             errstr, errstr_size);
                rd_kafka_wrunlock(rk);
                assert(!err);
                break;

        case RD_KAFKA_RESP_ERR_NOT_COORDINATOR:
        case RD_KAFKA_RESP_ERR_COORDINATOR_NOT_AVAILABLE:
                /* FIXME: re-query coordinator and retry */
                break;

        case RD_KAFKA_RESP_ERR_COORDINATOR_LOAD_IN_PROGRESS:
        case RD_KAFKA_RESP_ERR_CONCURRENT_TRANSACTIONS:
                /* FIXME: retry */
                break;
        case RD_KAFKA_RESP_ERR_TRANSACTIONAL_ID_AUTHORIZATION_FAILED:
        default:
                rd_kafka_txn_fatal_error(rk, err, "InitProducerId failed: %s",
                                         rd_kafka_err2str(err));
                /* FIXME: errstr? */
                break;
        }

        return RD_KAFKA_RESP_ERR_NO_ERROR;
}




/**
 * Should be called before the start of each new transaction. Note that prior to the first invocation
 * of this method, you must invoke {@link #initTransactions()} exactly one time.
 *
 * @throws IllegalStateException if no transactional.id has been configured or if {@link #initTransactions()}
 *         has not yet been invoked
 * @throws ProducerFencedException if another producer with the same transactional.id is active
 * @throws org.apache.kafka.common.errors.UnsupportedVersionException fatal error indicating the broker
 *         does not support transactions (i.e. if its version is lower than 0.11.0.0)
 * @throws org.apache.kafka.common.errors.AuthorizationException fatal error indicating that the configured
 *         transactional.id is not authorized. See the exception for more details
 * @throws KafkaException if the producer has encountered a previous fatal error or for any other unexpected error
 */
rd_kafka_resp_err_t rd_kafka_beginTransactions (rd_kafka_t *rk) {
        rd_kafka_resp_err_t err;

        rd_kafka_wrlock(rk);
        if ((err = rd_kafka_ensure_txn_initialized(rk, errstr, errstr_size)) ||
            (err = rd_kafka_ensure_txn_no_error(rk, errstr, errstr_size))) {
                rd_kafka_wrunlock(rk);
                return err;
        }

        err = rd_kafka_txn_set_state(rk, RD_KAFKA_TXN_STATE_IN_TRANSACTION,
                                     errstr, errstr_size);
        rd_kafka_wrunlock(rk);

        return err;
}

/**
 * Sends a list of specified offsets to the consumer group coordinator, and also marks
 * those offsets as part of the current transaction. These offsets will be considered
 * committed only if the transaction is committed successfully. The committed offset should
 * be the next message your application will consume, i.e. lastProcessedMessageOffset + 1.
 * <p>
 * This method should be used when you need to batch consumed and produced messages
 * together, typically in a consume-transform-produce pattern. Thus, the specified
 * {@code consumerGroupId} should be the same as config parameter {@code group.id} of the used
 * {@link KafkaConsumer consumer}. Note, that the consumer should have {@code enable.auto.commit=false}
 * and should also not commit offsets manually (via {@link KafkaConsumer#commitSync(Map) sync} or
 * {@link KafkaConsumer#commitAsync(Map, OffsetCommitCallback) async} commits).
 *
 * @throws IllegalStateException if no transactional.id has been configured or no transaction has been started
 * @throws ProducerFencedException fatal error indicating another producer with the same transactional.id is active
 * @throws org.apache.kafka.common.errors.UnsupportedVersionException fatal error indicating the broker
 *         does not support transactions (i.e. if its version is lower than 0.11.0.0)
 * @throws org.apache.kafka.common.errors.UnsupportedForMessageFormatException  fatal error indicating the message
 *         format used for the offsets topic on the broker does not support transactions
 * @throws org.apache.kafka.common.errors.AuthorizationException fatal error indicating that the configured
 *         transactional.id is not authorized. See the exception for more details
 * @throws KafkaException if the producer has encountered a previous fatal or abortable error, or for any
 *         other unexpected error
 */

rd_kafka_resp_err_t
rd_kafka_sendOffsetsToTransaction (rd_kafka_t *rk,
                                   rd_kafka_topic_partition_list_t *offsets,
                                   const char *consumer_group_id,
                                   char *errstr, size_t errstr_size) {
        rd_kafka_resp_err_t err;

        rd_kafka_wrlock(rk);
        if ((err = rd_kafka_ensure_txn_initialized(rk, errstr, errstr_size)) ||
            (err = rd_kafka_ensure_txn_no_error(rk, errstr_size))) {
                rd_kafka_wrunlock(rk);
                return err;
        }

        if (rk->rk_txn.state != RD_KAFKA_TXN_STATE_IN_TRANSACTION) {
                rd_snprintf(errstr, errstr_size,
                            "Can't send offsets to transaction because "
                            "producer is not in state IN_TRANSACTION "
                            "but %s",
                            rd_kafka_txn_state_names[rk->rk_txn.state]);
                return RD_KAFKA_RESP_ERR__STATE;
        }

        rd_kafka_dbg(rk, TXN, "TXNOFFSETS",
                     "Begin adding %d offset(s) for consumer group %s "
                     "to transaction",
                     offsets->cnt, consumer_group_id);

        rd_kafka_wrunlock(rk);

        /* FIXME: Send AddOffsetsToTxnRequest */

        return err;

}


/**
 * Commits the ongoing transaction. This method will flush any unsent records before actually committing the transaction.
 *
 * Further, if any of the {@link #send(ProducerRecord)} calls which were part of the transaction hit irrecoverable
 * errors, this method will throw the last received exception immediately and the transaction will not be committed.
 * So all {@link #send(ProducerRecord)} calls in a transaction must succeed in order for this method to succeed.
 *
 * @throws IllegalStateException if no transactional.id has been configured or no transaction has been started
 * @throws ProducerFencedException fatal error indicating another producer with the same transactional.id is active
 * @throws org.apache.kafka.common.errors.UnsupportedVersionException fatal error indicating the broker
 *         does not support transactions (i.e. if its version is lower than 0.11.0.0)
 * @throws org.apache.kafka.common.errors.AuthorizationException fatal error indicating that the configured
 *         transactional.id is not authorized. See the exception for more details
 * @throws KafkaException if the producer has encountered a previous fatal or abortable error, or for any
 *         other unexpected error
 */
rd_kafka_resp_err_t
rd_kafka_commitTransaction (rd_kafka_t *rk,
                            char *errstr, size_t errstr_size) {
        rd_kafka_resp_err_t err;

        rd_kafka_wrlock(rk);
        if ((err = rd_kafka_ensure_txn_initialized(rk, errstr, errstr_size)) ||
            (err = rd_kafka_ensure_txn_no_error(rk, errstr_size))) {
                rd_kafka_wrunlock(rk);
                return err;
        }

        if ((err = rd_kafka_txn_set_state(
                     rk, RD_KAFKA_TXN_STATE_COMMITTING_TRANSACTION,
                     errstr, errstr_size))) {
                rd_kafka_wrunlock(rk);
                return err;
        }

        // FIXME: continue here..

}

