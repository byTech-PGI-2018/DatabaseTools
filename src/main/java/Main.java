import com.google.api.core.ApiFuture;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.Timestamp;
import com.google.cloud.firestore.*;

import com.google.common.collect.Lists;
import com.google.firebase.FirebaseApp;
import com.google.firebase.FirebaseOptions;
import com.google.firebase.cloud.FirestoreClient;
import org.apache.commons.io.FileUtils;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Array;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class Main {
    final static String projectId = "got2eat-65f04";

    final static String[] file = {"ingredientes.json", "recipes.json", "users.json"};

    private static boolean jsonObjectToList(JSONObject recipe, String key, List<String> list) {
        try{
            JSONObject json = recipe.getJSONObject(key);

            Iterator<String> keys = json.keys();
            while(keys.hasNext()){
                list.add(json.get(keys.next()).toString());
            }

            return true;
        } catch (Exception e){
            System.out.println("Failed to get JSON Object with key: " + key);
            return false;
        }
    }

    private static void getItems(Firestore db, String option){
        System.out.println("Getting " + option + "...");
        //Get all ingredients
        JSONObject json = new JSONObject();

        ApiFuture<QuerySnapshot> query = db.collection(option).get();
        try{
            QuerySnapshot querySnapshot = query.get();
            querySnapshot.forEach((snapshot)->{
                if (option.matches("ingredientes")){
                    json.put(snapshot.getId(), "");
                }

                else if (option.matches("receitas")){
                    JSONObject recipe = new JSONObject();
                    JSONArray recipeIngredients = null;

                    //System.out.println("-------NEW---------");

                    if (snapshot.get("ingredients") != null){
                        System.out.println((ArrayList<String>) snapshot.get("ingredients"));
                        recipeIngredients = new JSONArray(((ArrayList<String>) snapshot.get("ingredients")).toArray());
                        recipe.put("ingredients", recipeIngredients);
                    }

                    recipe.put("name", snapshot.get("name"));
                    recipe.put("porção", snapshot.get("porção"));

                    JSONArray recipePreparation = null;

                    if (snapshot.get("preparação") != null){
                        System.out.println((ArrayList<String>) snapshot.get("preparação"));
                        recipePreparation = new JSONArray(((ArrayList<String>) snapshot.get("preparação")).toArray());
                        recipe.put("preparação", recipePreparation);
                    }

                    JSONArray recipeQuantity = null;

                    if (snapshot.get("quantidade") != null){
                        System.out.println((ArrayList<String>) snapshot.get("quantidade"));
                        recipeQuantity = new JSONArray(((ArrayList<String>) snapshot.get("quantidade")).toArray());
                        recipe.put("quantidade", recipeQuantity);
                    }

                    recipe.put("tempo", snapshot.get("tempo"));
                    try{
                        recipe.put("timestamp", ((Timestamp)snapshot.get("timestamp")).toString());
                    } catch (Exception e){
                        System.out.println("Failed to add timestamp: "  + e);
                    }

                    json.put(snapshot.getId(), recipe);
                }

                else if (option.matches("users")){
                    JSONObject user = new JSONObject();

                    JSONArray logs = null;
                    if (snapshot.get("logs") != null){
                        System.out.println((ArrayList<String>) snapshot.get("logs"));
                        logs = new JSONArray(((ArrayList<String>) snapshot.get("logs")).toArray());
                        user.put("logs", logs);
                    }

                    JSONArray saved = null;
                    if (snapshot.get("saved") != null){
                        System.out.println((ArrayList<String>) snapshot.get("saved"));
                        saved = new JSONArray(((ArrayList<String>) snapshot.get("saved")).toArray());
                        user.put("saved", saved);
                    }

                    user.put("email", snapshot.get("email"));
                    user.put("firstname", snapshot.get("firstname"));
                    user.put("username", snapshot.get("username"));

                    try{
                        user.put("lastlogin", ((Timestamp)snapshot.get("lastlogin")).toString());
                    } catch (Exception e){
                        System.out.println("Failed to add lastlogin: "  + e);
                    }

                    json.put(snapshot.getId(), user);
                }
            });
        } catch (Exception e){
            System.out.println(e);
            System.exit(2);
        }

        try{
            System.out.println("Writing to " + option + ".json" + "...");
            FileUtils.writeStringToFile(new File(option+".json"), json.toString(), StandardCharsets.UTF_8);
        } catch (Exception e){
            System.out.println(e);
            System.exit(3);
        }
    }

    private static void postItems(Firestore db, String inputFile){
        Logger logger = Logger.getLogger("PostItems");
        FileHandler fh;

        //Open logging file
        try {

            // This block configure the logger with handler and formatter
            fh = new FileHandler("log.txt");
            logger.addHandler(fh);
            SimpleFormatter formatter = new SimpleFormatter();
            fh.setFormatter(formatter);

        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        //Open JSON Lines file
        File jsonFile = new File(inputFile);
        if (!jsonFile.exists()){
            System.out.println("File is invalid");
            System.exit(3);
        }

        //Read line by line
        try(Scanner scanner = new Scanner(jsonFile)){
            int lineCounter = 0;

            while(scanner.hasNextLine()){
                lineCounter++;
                String recipeString = scanner.nextLine();
                JSONObject recipe;

                //Check if we have a valid JSON Object
                try{
                    recipe = new JSONObject(recipeString);
                } catch (Exception e){
                    System.out.println("Failed to create JSON from input: " + recipeString);
                    System.out.println(e);
                    logger.warning("Failed to create JSON from input. Line " + lineCounter + "\nException: " + e);

                    continue;
                }

                //Extract ingredient list
                List<String> ingredientList = new ArrayList<>();
                if (!jsonObjectToList(recipe, "ingredients", ingredientList) || ingredientList.isEmpty()){
                    System.out.println("Failed to extract ingredient list");
                    logger.warning("Failed to extract ingredient list. Line " + lineCounter);

                    continue;
                }

                //Extract quantity list
                List<String> quantityList = new ArrayList<>();
                if (!jsonObjectToList(recipe, "quantidade", quantityList) || quantityList.isEmpty()){
                    System.out.println("Failed to extract quantity list");
                    logger.warning("Failed to extract quantity list. Line " + lineCounter);

                    continue;
                }

                //Extract preparation list
                List<String> preparationList = new ArrayList<>();
                if (!jsonObjectToList(recipe, "preparação", preparationList) || preparationList.isEmpty()){
                    System.out.println("Failed to extract preparation list");
                    logger.warning("Failed to extract preparation list. Line " + lineCounter);

                    continue;
                }

                // Add document data with auto-generated id.
                Map<String, Object> data = new HashMap<>();
                try{
                    data.put("name", recipe.getString("name"));
                    data.put("porção", recipe.getString("porção"));
                    data.put("tempo", recipe.getString("tempo"));
                    data.put("vegan", recipe.getBoolean("vegan"));
                    data.put("ingredients", ingredientList);
                    data.put("quantidade", quantityList);
                    data.put("preparação", preparationList);
                    data.put("timestamp", FieldValue.serverTimestamp());
                } catch (Exception e){
                    System.out.println("Failed to create data map for line " + lineCounter);
                    System.out.println(e);
                    logger.warning("Failed to create data map for line " + lineCounter);
                    logger.warning(e.toString());
                }

                logger.info("Successfully created data map for line " + lineCounter);

                //Write data to firestore
                ApiFuture<DocumentReference> future = db.collection("receitas").add(data);
                String docId = future.get().getId();

                logger.info("Successfully uploaded recipe in line " + lineCounter);
                logger.info("Successfully uploaded new recipe to cloud with id: " + docId);
            }

        } catch (FileNotFoundException e){
            System.out.println("File is invalid: " + e);
        } catch (Exception e){
            System.out.println("Error: " + e);
        }
    }

    public static void main(String[] args){
        // Use the application default credentials
        GoogleCredentials credentials = null;
        try{
            credentials = GoogleCredentials.getApplicationDefault();
        }
        catch (Exception e){
            System.out.println(e);
            System.exit(1);
        }
        FirebaseOptions options = new FirebaseOptions.Builder()
                .setCredentials(credentials)
                .setProjectId(projectId)
                .build();
        FirebaseApp.initializeApp(options);

        Firestore db = FirestoreClient.getFirestore();

        System.out.println("Database object: " + db);

        //getItems(db, "users");
        postItems(db, "/home/filipe/Desktop/Data/final/recipes.jsonl");
    }
}
