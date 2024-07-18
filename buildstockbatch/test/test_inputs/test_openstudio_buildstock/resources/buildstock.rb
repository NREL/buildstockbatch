# frozen_string_literal: true
class Version
  ResStock_Version = '2000.1.0' # Version of ResStock
  BuildStockBatch_Version = '2023.5.0' # Minimum required version of BuildStockBatch

  def self.check_buildstockbatch_version
    if ENV.keys.include?('BUILDSTOCKBATCH_VERSION') # buildstockbatch is installed
      bsb_version = ENV['BUILDSTOCKBATCH_VERSION']
      if bsb_version < BuildStockBatch_Version
        fail "BuildStockBatch version #{BuildStockBatch_Version} or above is required. Found version: #{bsb_version}"
      end
    end
  end
end
