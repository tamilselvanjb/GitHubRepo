package com;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.forddirect.services.debug.data.check.IDataCheck;
import com.trilogy.forddirect.util.environment.EnvironmentInfo;

/**
 * Class to encapsulate the results of all checks for all vehicles
 * At present we are recording and reporting just the failures. In case
 * our reporting needs require us to store details for successful/skipped
 * tests as well then we can evolve the data structure appropriately. In
 * such a case, we can also change the strategy used for persisting the
 * results on disk to handle the appropriate expected data volume and details
 * 
 * @author Amit Arora
 */
public class ConsolidatedDataHealthCheckResult
        implements Serializable {

    private static final long serialVersionUID = 1L;
	// Tamil added new comments

    /**
     * Path where the results of data check can be persisted
     * so that they can be reused across server restarts
     */
    private static final String DATA_HEALTHCHECK_RESULTS_FOLDER = "DATA_HEALTHCHECK_RESULTS_FOLDER";
    private static final String DEFAULT_DATA_HEALTHCHECK_RESULTS_FOLDER = ".";

    private static String g_resultsFolder;

    private static Logger logger = Logger
            .getLogger(ConsolidatedDataHealthCheckResult.class);

    private long runEndTime; // end time for the data check run
    private boolean overallSuccess; // whether the data check run was successful
                                    // or not

    // map of checkName -> results of that check for all vehicles
    private Map<String, DataCheckResultsForVehicles> healthCheckVehicleDetailsMap = new HashMap<String, DataCheckResultsForVehicles>();

    static {
        try {
            g_resultsFolder = EnvironmentInfo.getEnvironmentInfo()
                    .getParameter(DATA_HEALTHCHECK_RESULTS_FOLDER);
        } catch(Exception e) {
            logger.error(
                    "Could not load/read DATA_HEALTHCHECK_RESULTS_FOLDER property. Using default : "
                            + DEFAULT_DATA_HEALTHCHECK_RESULTS_FOLDER, e);
            g_resultsFolder = DEFAULT_DATA_HEALTHCHECK_RESULTS_FOLDER;
        }
        
        // Create this folder if it doesn't already exist
        File resultsFolder = new File(g_resultsFolder);
        if(!resultsFolder.exists()) {
            try {
                resultsFolder.mkdirs();
            }
            catch(Exception e) {
                logger.error("Error occurred while trying to create the folder : " + g_resultsFolder, e);
                throw new IllegalStateException("Unable to create folder : " + g_resultsFolder + " for storing data health check results", e);
            }
        }
        
        logger.info("Using folder : " + g_resultsFolder + " for storing data health check results");
    }

    /**
     * Store the results for the specified check
     * 
     * @param checkId Id of the check that was performed
     * @param result Result for all vehicles for which the check was performed
     */
    public void storeCheckResults(String checkId,
            DataCheckResultsForVehicles result) {
        healthCheckVehicleDetailsMap.put(checkId, result);
    }
    
    /**
     * Clears the last run records for the specified data check in totality
     * 
     * @param checkId
     */
    public static void clearCheckResults(String checkId) {
        String resultFilePath = getResultsFileName(checkId);
        File resultFile = new File(resultFilePath);
        if(resultFile.exists()) {
            boolean deleteSuccess = resultFile.delete();
            if(!deleteSuccess) {
                throw new IllegalStateException("Unable to delete results file for data check with id : " + checkId);
            }
        }
        else {
            throw new IllegalArgumentException("No results found for data check with id : " + checkId);
        }
    }

    /**
     * A String with details of all checks which failed for some vehicles
     * 
     * @return A String with details of all checks which failed for some
     *         vehicles.
     *         An empty string if none of the checks failed for any vehicle
     */
    public String getFailureMessage() {
        StringBuilder consolidatedStatusMessage = new StringBuilder();

        // Iterate over all checks one by one
        for(String check : healthCheckVehicleDetailsMap.keySet()) {
            DataCheckResultsForVehicles checkResults = healthCheckVehicleDetailsMap
                    .get(check);

            // Include the results of this check if it had failed
            if(!checkResults.isSuccess()) {
                if(consolidatedStatusMessage.length() > 0) {
                    consolidatedStatusMessage.append(";");
                }

                consolidatedStatusMessage.append(checkResults
                        .getFailureMessage());
            }
        }

        return consolidatedStatusMessage.toString();
    }

    /**
     * A routine to persist the results on to the disk.
     * Results for individual data checks are persisted in
     * different files
     * 
     * @throws IOException
     */
    public void persist() {
        for(DataCheckResultsForVehicles result : healthCheckVehicleDetailsMap
                .values()) {
            String resultFilePath = getResultsFileName(result.getCheckId());
            try {
                result.persist(resultFilePath);
            } catch(IOException e) {
                logger.error("Error while persisting results for check at : "
                        + resultFilePath, e);
            }
        }
    }

    /**
     * Get the path where the results for the check with specified
     * name should be stored
     * 
     * @param checkId id of the data check for which results need to be stored
     * @return
     */
    public static String getResultsFileName(String checkId) {
        return g_resultsFolder + "/" + checkId + "_Results.ser";
    }

    /**
     * This routine is used to load the consolidated results of the
     * latest data health check run from disk. Results for individual
     * data checks are persisted in different files. This routine
     * loads all the individual persisted results file and returns a
     * consolidated result object corresponding to those. If no persisted
     * results are found, it returns null
     * 
     * @return
     * @throws IOException
     */
    public static ConsolidatedDataHealthCheckResult loadLatestResults()
            throws IOException {
        ConsolidatedDataHealthCheckResult consolidatedResults = new ConsolidatedDataHealthCheckResult();

        boolean anyResultsFound = false;
        boolean overallSuccess = true;
        long maxEndTime = 0;
        Map<String, IDataCheck> allChecks = DataHealthCheckTimerTask
                .getAllDataChecks();
        for(IDataCheck check : allChecks.values()) {
            String resultsFilePath = getResultsFileName(check.getId());
            DataCheckResultsForVehicles results = DataCheckResultsForVehicles
                    .loadResultsIfPresent(resultsFilePath);
            if(results != null) {
                anyResultsFound = true;
                consolidatedResults.storeCheckResults(check.getId(), results);
                overallSuccess = overallSuccess & results.isSuccess();

                // Now calculate the end time
                if(results.getEndTime() > maxEndTime) {
                    maxEndTime = results.getEndTime();
                }
            }
        }

        // Check if any results were found then set the fields
        if(anyResultsFound) {
            consolidatedResults.overallSuccess = overallSuccess;
            consolidatedResults.runEndTime = maxEndTime;
        } else {
            return null;
        }

        return consolidatedResults;
    }

    public long getRunEndTime() {
        return runEndTime;
    }

    public void setRunEndTime(long runEndTime) {
        this.runEndTime = runEndTime;
    }

    public boolean isOverallSuccess() {
        return overallSuccess;
    }

    public void setOverallSuccess(boolean overallSuccess) {
        this.overallSuccess = overallSuccess;
    }

    public Map<String, DataCheckResultsForVehicles> getHealthCheckVehicleDetailsMap() {
        return healthCheckVehicleDetailsMap;
    }
}
