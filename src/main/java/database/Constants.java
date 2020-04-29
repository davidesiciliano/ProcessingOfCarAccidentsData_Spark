package database;

public class Constants {
    // DATE, TIME, BOROUGH, ZIP CODE, LATITUDE, LONGITUDE, LOCATION, ON STREET NAME,
    // CROSS STREET NAME, OFF STREET NAME, NUMBER OF PERSONS INJURED, NUMBER OF PERSONS KILLED,
    // NUMBER OF PEDESTRIANS INJURED, NUMBER OF PEDESTRIANS KILLED, NUMBER OF CYCLIST INJURED,
    // NUMBER OF CYCLIST KILLED, NUMBER OF MOTORIST INJURED, NUMBER OF MOTORIST KILLED,
    // CONTRIBUTING FACTOR VEHICLE 1, CONTRIBUTING FACTOR VEHICLE 2, CONTRIBUTING FACTOR VEHICLE 3,
    // CONTRIBUTING FACTOR VEHICLE 4, CONTRIBUTING FACTOR VEHICLE 5, UNIQUE KEY, VEHICLE TYPE CODE 1,
    // VEHICLE TYPE CODE 2, VEHICLE TYPE CODE 3, VEHICLE TYPE CODE 4,VEHICLE TYPE CODE 5
    public static final String DATE = "Date";
    public static final String TIME = "Time";
    public static final String BOROUGH = "Borough";
    public static final String ZIPCODE = "ZipCode";
    public static final String LATITUDE = "Latitude";
    public static final String LONGITUDE = "Longitude";
    public static final String LOCATION = "Location";
    public static final String ON_STREET_NAME = "OnStreetName";
    public static final String CROSS_STREET_NAME = "CrossStreetName";
    public static final String OFF_STREET_NAME = "OffStreetName";
    public static final String NUMBER_OF_PERSONS_INJURED = "NumberOfPersonsInjured";
    public static final String NUMBER_OF_PERSONS_KILLED = "NumberOfPersonsKilled";
    public static final String NUMBER_OF_PEDESTRIANS_INJURED = "NumberOfPedestriansInjured";
    public static final String NUMBER_OF_PEDESTRIANS_KILLED = "NumberOfPedestriansKilled";
    public static final String NUMBER_OF_CYCLIST_INJURED = "NumberOfCyclistInjured";
    public static final String NUMBER_OF_CYCLIST_KILLED = "NumberOfCyclistKilled";
    public static final String NUMBER_OF_MOTORIST_INJURED = "NumberOfMotoristInjured";
    public static final String NUMBER_OF_MOTORIST_KILLED = "NumberOfMotoristKilled";
    public static final String CONTRIBUTING_FACTOR_VEHICLE_1 = "ContributingFactorVehicle1";
    public static final String CONTRIBUTING_FACTOR_VEHICLE_2 = "ContributingFactorVehicle2";
    public static final String CONTRIBUTING_FACTOR_VEHICLE_3 = "ContributingFactorVehicle3";
    public static final String CONTRIBUTING_FACTOR_VEHICLE_4 = "ContributingFactorVehicle4";
    public static final String CONTRIBUTING_FACTOR_VEHICLE_5 = "ContributingFactorVehicle5";
    public static final String UNIQUE_KEY = "UniqueKey";
    public static final String VEHICLE_TYPE_CODE_1 = "VehicleTypeCode1";
    public static final String VEHICLE_TYPE_CODE_2 = "VehicleTypeCode2";
    public static final String VEHICLE_TYPE_CODE_3 = "VehicleTypeCode3";
    public static final String VEHICLE_TYPE_CODE_4 = "VehicleTypeCode4";
    public static final String VEHICLE_TYPE_CODE_5 = "VehicleTypeCode5";

    //Added columns
    public static final String WEEK = "Week";
    public static final String YEAR = "Year";
    public static final String NUMBER_INJURED = "NumberInjured";
    public static final String SUM_NUMBER_INJURED = "sum(NumberInjured)";
    public static final String NUMBER_KILLED = "NumberKilled";
    public static final String SUM_NUMBER_KILLED = "sum(NumberKilled)";
    public static final String NUMBER_ACCIDENTS = "NumberAccidents";
    public static final String NUMBER_LETHAL_ACCIDENTS = "NumberLethalAccidents";
    public static final String SUM_NUMBER_LETHAL_ACCIDENTS = "sum(NumberLethalAccidents)";
    public static final String AVERAGE_NUMBER_LETHAL_ACCIDENTS = "AvgNumberLethalAccidents";
    public static final String CONTRIBUTING_FACTOR = "ContributingFactor";

    private Constants() {
        throw new AssertionError();
    }
}
