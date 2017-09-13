(ns onyx.cep.pattern-matcher
  (:require [clojure.edn :as edn]
            [metamorphic.runtime :as rt]
            [metamorphic.match-buffer :refer [->Event ->MatchBufferStage]]
            [taoensso.nippy :as nippy]))

(nippy/extend-freeze metamorphic.match_buffer.Event ::Event
 [{:keys [payload state-name pattern-name pointers refs]} data-output]
 (let [bs (nippy/fast-freeze [payload state-name pattern-name pointers @refs])] 
   (.writeInt data-output (alength bs))
   (.write data-output bs)))

(nippy/extend-thaw ::Event
 [data-input]
 (let [len (.readInt data-input)
       bs (byte-array len)
       _ (.readFully data-input bs) 
       [payload state-name pattern-name patterns refs] (nippy/fast-thaw bs)]
   (->Event payload state-name pattern-name patterns (atom refs))))

(defrecord MatchState [pending-states match-buffer matches timed-out-matches])

(nippy/extend-freeze
 onyx.cep.pattern_matcher.MatchState ::MatchState
 [{:keys [pending-states match-buffer matches timed-out-matches]} data-output]
 (let [bs (nippy/fast-freeze (list pending-states match-buffer matches timed-out-matches))] 
   (.writeInt data-output (alength bs))
   (.write data-output bs)))

(nippy/extend-thaw ::MatchState
 [data-input]
 (let [len (.readInt data-input)
       bs (byte-array len)
       _ (.readFully data-input bs) 
       [pending-states match-buffer matches timed-out-matches] (nippy/fast-thaw bs)]
   (->MatchState pending-states match-buffer matches timed-out-matches)))

(defn compact [{:keys [pending-states match-buffer matches timed-out-matches]}] 
  (->MatchState pending-states match-buffer matches timed-out-matches))

(defn init-locals [window]
  {:runtime (rt/initialize-runtime (:cep/pattern-sequence window))})

(defn aggregation-fn-init [window]
  (compact (:runtime window)))

(defn aggregation-fn [window segment]
  (let [k (second (:window/aggregation window))]
    (get segment k)))

(defn aggregation-apply-log [window {:keys [pending-states match-buffer matches timed-out-matches]} v]
  (-> window
      :runtime 
      (assoc :pending-states pending-states)
      (assoc :match-buffer match-buffer)
      (assoc :matches matches)
      (assoc :timed-out-matches timed-out-matches)
      (rt/evaluate-event v)
      (compact)))

(defn super-aggregation [window state-1 state-2]
  (throw (ex-info "Pattern matching not yet supported for session windows." {})))

(def ^:export pattern-matcher
  {:aggregation/init-locals init-locals
   :aggregation/init aggregation-fn-init
   :aggregation/create-state-update aggregation-fn
   :aggregation/apply-state-update aggregation-apply-log
   :aggregation/super-aggregation-fn super-aggregation})
