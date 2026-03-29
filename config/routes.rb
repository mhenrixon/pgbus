# frozen_string_literal: true

Pgbus::Engine.routes.draw do
  root to: "dashboard#show"

  resources :queues, only: %i[index show], param: :name do
    member do
      post :purge
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

  namespace :api do
    get :stats, to: "stats#show"
  end
end
