(ns onyx.cep.pattern-matcher
  (:require [onyx-cep.runtime :as rt]))

(def durable-ks [:pending-states :match-buffer :matches :timed-out-matches])

(defn init-locals [window]
  {:runtime (rt/initialize-runtime (:cep/pattern-sequence window))})

(defn aggregation-fn-init [window]
  (select-keys (:runtime window) durable-ks))

(defn aggregation-fn [window segment]
  (let [k (second (:window/aggregation window))]
    (get segment k)))

(defn aggregation-apply-log [window incremental-rt v]
  (select-keys (rt/evaluate-event (into (:runtime window) incremental-rt) v)
               durable-ks))

(defn super-aggregation [window state-1 state-2]
  (throw (ex-info "Pattern matching not yet supported for session windows." {})))

(def ^:export pattern-matcher
  {:aggregation/init-locals init-locals
   :aggregation/init aggregation-fn-init
   :aggregation/create-state-update aggregation-fn
   :aggregation/apply-state-update aggregation-apply-log
   :aggregation/super-aggregation-fn super-aggregation})
