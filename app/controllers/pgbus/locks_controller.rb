# frozen_string_literal: true

module Pgbus
  class LocksController < ApplicationController
    def index
      @locks = data_source.job_locks
    end

    def discard
      count = data_source.discard_lock(params[:id])
      if count.positive?
        redirect_to locks_path, notice: t("pgbus.locks.index.lock_discarded")
      else
        redirect_to locks_path, alert: t("pgbus.locks.index.lock_discard_failed")
      end
    end

    def discard_selected
      keys = Array(params[:lock_keys]).reject(&:blank?)
      if keys.empty?
        redirect_to locks_path, alert: t("pgbus.locks.index.none_selected")
        return
      end

      count = data_source.discard_locks(keys)
      redirect_to locks_path, notice: t("pgbus.locks.index.locks_discarded", count: count)
    end

    def discard_all
      count = data_source.discard_all_locks
      redirect_to locks_path, notice: t("pgbus.locks.index.all_locks_discarded", count: count)
    end
  end
end
