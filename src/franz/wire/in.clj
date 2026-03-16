(ns franz.wire.in
  (:require [schema.core :as s]))

(s/defschema CreateClusterRequest
  {:name                      s/Str
   :bootstrap-url             s/Str
   (s/optional-key :labels)   {s/Str s/Str}})

(s/defschema UpdateClusterRequest
  {(s/optional-key :bootstrap-url) s/Str
   (s/optional-key :labels)        {s/Str s/Str}})

(s/defschema PaginationParams
  {(s/optional-key :page) s/Int
   (s/optional-key :size) s/Int})
