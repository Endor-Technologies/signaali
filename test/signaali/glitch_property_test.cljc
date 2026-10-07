(ns signaali.glitch-property-test
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.test.check :as tc]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [signaali.reactive :as sr]))

(deftest missed-change-repro
  (testing "a maybe-stale memo sees a change its source propagated before a later no-op run"
    (let [db (sr/create-state {:a 1 :b 1})
          x (sr/create-memo (fn [] (:a @db)))
          d (sr/create-memo (fn [] (* 10 @x)))
          other (sr/create-memo (fn [] @x))]
      @d
      @other
      (swap! db assoc :a 2)
      @other
      (swap! db assoc :b 2)
      @other
      (is (= 20 @d)))))

;; A fixed DAG over a 4-slot state, mixing memos (with propagation filter) and
;; derived nodes (without), plus one node with dynamic dependencies.
(def node-specs
  ;; id -> [kind deps f]
  [[:l0 :memo [:s0] (fn [a] (mod a 3))]
   [:l1 :derived [:s1] (fn [a] (mod a 3))]
   [:l2 :memo [:s2] (fn [a] (quot a 2))]
   [:l3 :memo [:s3] identity]
   [:m0 :memo [:l0 :l1] +]
   [:m1 :derived [:l1 :l2] *]
   [:m2 :memo [:l2 :l3] max]
   [:m3 :memo [:l0 :l3] -]
   [:t0 :memo [:m0 :m1 :m2] (fn [a b c] (+ a (* 2 b) (* 3 c)))]
   [:t1 :derived [:m2 :m3] (fn [a b] (mod (+ a b) 4))]
   [:t2 :memo [:t0 :t1] (fn [a b] (+ a b))]])

(defn build-graph []
  (let [db (sr/create-state [0 0 0 0])
        slots (into {}
                    (for [i (range 4)]
                      [(keyword (str "s" i))
                       (sr/create-memo (fn [] (nth @db i)))]))
        nodes (reduce (fn [nodes [id kind deps f]]
                        (let [run (fn [] (apply f (map (fn [d] @(nodes d)) deps)))
                              node (case kind
                                     :memo    (sr/create-memo run)
                                     :derived (sr/create-derived run))]
                          (assoc nodes id node)))
                      slots
                      node-specs)
        ;; dynamic dependencies: reads :m0 or :m3 depending on :l2
        dyn (sr/create-memo (fn [] (if (even? @(nodes :l2)) @(nodes :m0) @(nodes :m3))))]
    {:db db :nodes (assoc nodes :dyn dyn)}))

(defn expected-values [state]
  (let [base (into {} (map-indexed (fn [i v] [(keyword (str "s" i)) v]) state))
        vals (reduce (fn [vals [id _ deps f]]
                       (assoc vals id (apply f (map vals deps))))
                     base
                     node-specs)]
    (assoc vals :dyn (if (even? (vals :l2)) (vals :m0) (vals :m3)))))

(def node-ids (into [:s0 :s1 :s2 :s3 :dyn] (map first node-specs)))

(def op-gen
  (gen/one-of
    [(gen/tuple (gen/return :set) (gen/choose 0 3) (gen/choose 0 5))
     (gen/tuple (gen/return :deref) (gen/elements node-ids))]))

(defn run-ops [ops]
  (let [{:keys [db nodes]} (build-graph)]
    (reduce (fn [state [op a b]]
              (case op
                :set   (do (swap! db assoc a b)
                           (assoc state a b))
                :deref (let [actual @(nodes a)
                             expected ((expected-values state) a)]
                         (if (= actual expected)
                           state
                           (reduced {:mismatch a :actual actual :expected expected})))))
            [0 0 0 0]
            (concat ops (map (fn [id] [:deref id]) node-ids)))))

(def no-stale-reads
  (prop/for-all [ops (gen/vector op-gen 0 40)]
                (not (contains? (run-ops ops) :mismatch))))

(deftest every-read-matches-a-fresh-recomputation
  (let [result (tc/quick-check 2000 no-stale-reads)]
    (is (:pass? result) (pr-str (select-keys result [:shrunk :fail])))))

;; Effects are how UI watchers consume nodes: after a write, the stale effects
;; re-run in a batch and must observe the latest values.
(def effects-see-latest
  (prop/for-all [ops (gen/vector op-gen 0 40)]
                (let [{:keys [db nodes]} (build-graph)
                      seen (atom {})
                      _ (doseq [id [:t2 :dyn :m1 :l1]]
                          (let [e (sr/create-effect (fn [] (swap! seen assoc id @(nodes id))))]
                            @e))
                      state (reduce (fn [state [op a b]]
                                      (case op
                                        :set   (do (swap! db assoc a b) (assoc state a b))
                                        :deref (do @(nodes a) state)))
                                    [0 0 0 0]
                                    ops)]
                  (sr/re-run-stale-effectful-nodes)
                  (let [expected (expected-values state)]
                    (every? (fn [id] (= (@seen id) (expected id))) [:t2 :dyn :m1 :l1])))))

(deftest effects-observe-latest-values
  (let [result (tc/quick-check 2000 effects-see-latest)]
    (is (:pass? result) (pr-str (select-keys result [:shrunk :fail])))))
