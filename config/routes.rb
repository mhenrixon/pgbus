# frozen_string_literal: true

Pgbus::Engine.routes.draw do
  root to: "dashboard#show"

  resources :queues, only: %i[index show destroy], param: :name do
    member do
      post :purge
      post :pause
      post :resume
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
    end
  end

  resources :outbox, only: [:index], controller: "outbox"
  resources :locks, only: [:index]
  resource :insights, only: [:show], controller: "insights"

  get :set_locale, to: "locale#update"

  namespace :api do
    get :stats, to: "stats#show"
    get :insights, to: "insights#show"
  end
end
