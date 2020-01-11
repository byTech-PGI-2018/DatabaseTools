import com.google.api.core.ApiFuture;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.Timestamp;
import com.google.cloud.firestore.Firestore;

import com.google.cloud.firestore.QueryDocumentSnapshot;
import com.google.cloud.firestore.QuerySnapshot;
import com.google.common.collect.Lists;
import com.google.firebase.FirebaseApp;
import com.google.firebase.FirebaseOptions;
import com.google.firebase.cloud.FirestoreClient;
import org.apache.commons.io.FileUtils;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.File;
import java.lang.reflect.Array;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Main {
    final static String projectId = "got2eat-65f04";

    final static String[] file = {"ingredientes.json", "recipes.json", "users.json"};

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

                    System.out.println("-------NEW---------");

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

        getItems(db, "users");
    }
}
