(defproject org.onyxplatform/onyx-cep "0.11.0.0-SNAPSHOT"
  :description "Complex event processing for Onyx via Metamorphic."
  :url "https://github.com/onyx-platform/onyx-cep"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.9.0-alpha17"]
                 [org.onyxplatform/onyx "0.11.0-SNAPSHOT"]
                 [org.onyxplatform/metamorphic "0.1.0-SNAPSHOT"]]
  :profiles {:dev {:resource-paths ["test-resources/"]}})
