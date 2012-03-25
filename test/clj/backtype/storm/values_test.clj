(ns backtype.storm.values-test
  (:use [clojure test])
  (:import [java.util ArrayList])
  (:import [backtype.storm.tuple Values]))

(deftest test-constructor
  (let [values (Values. (object-array '("foo" "bar")))]
    (is (instance? ArrayList values))
    (is (.contains values "foo"))
    (is (.contains values "bar"))))

