# frozen_string_literal: true

Pgbus::Engine.routes.draw do
  root to: "dashboard#show"

  resources :queues, only: %i[index show destroy], param: :name do
    member do
      post :purge
      post :pause
      post :resume
      post :retry_message
      post :discard_message
    end
  end

  resources :jobs, only: %i[index show] do
    member do
      post :retry
      post :discard
    end
    collection do
      post :retry_all
      post :discard_all
      post :discard_all_enqueued
      post :discard_selected_failed
      post :discard_selected_enqueued
    end
  end

  resources :recurring_tasks, only: %i[index show] do
    member do
      post :toggle
      post :enqueue
    end
  end

  resources :processes, only: [:index]

  resources :events, only: %i[index show] do
    member do
      post :replay
    end
  end

  resources :dead_letter, only: %i[index show], path: "dlq" do
    member do
      post :retry
      post :discard
    end
    collection do
      post :retry_all
      post :discard_all
      post :discard_selected
    end
  end

  resources :outbox, only: [:index], controller: "outbox"
  resources :locks, only: [:index] do
    member do
      post :discard
    end
    collection do
      post :discard_selected
      post :discard_all
    end
  end
  resource :insights, only: [:show], controller: "insights"

  get :set_locale, to: "locale#update"

  namespace :api do
    get :stats, to: "stats#show"
    get :insights, to: "insights#show"
  end

  scope :frontend, controller: :frontends, defaults: { version: Pgbus::VERSION.tr(".", "-") } do
    get "modules/:version/:id", action: :module, as: :frontend_module, constraints: { format: "js" }
    get "static/:version/:id", action: :static, as: :frontend_static
  end
end
