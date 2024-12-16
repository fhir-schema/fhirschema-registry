# htmx, middleware and layouts

;;TODO: idea of lazy eval based on layout and slots

(defn handler [context request]
  (respond data {:layout-slot data-fn,  layout-slot: data-fn}))

;; in respond
we can get layouts which should be applied to this respond
or skip layouts if htmx request specific

we can register layouts on routes

(register-layout context {:methods [] :path "/" :fn #'app-layout})
(register-layout context {:methods [] :path "/admin" :fn #'admin-layout})
(register-layout context {:methods [] :path "/admin/user" :fn #'users-layout})
(register-layout context {:methods [] :path "/admin/user/:id" :fn #'user-layout})

will be evaled

;; apply user-layout
;; apply users-layout
;; apply admin-layout
;; apply app-layout


