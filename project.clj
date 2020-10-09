(defproject osrs-tileman-server "0.1.0-SNAPSHOT"
  :description "Server for Conor Leckey's tileman mode Runelite plugin for Old School Runescape"
  :license {:name "BSD 2-Clause License"
            :url "https://opensource.org/licenses/BSD-2-Clause"}
  :dependencies [[compojure "1.6.2"]
                 [http-kit "2.5.0"]
                 [ring/ring-core "1.8.2"]
                 [ring/ring-defaults "0.3.2"]
                 [metosin/ring-http-response "0.9.1"]
                 [mount "0.1.16"]
                 [org.clojure/clojure "1.10.0"]
                 [org.clojure/core.async "1.3.610"]
                 [org.clojure/data.json "1.0.0"]]
  :main ^:skip-aot osrs-tileman-server.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
