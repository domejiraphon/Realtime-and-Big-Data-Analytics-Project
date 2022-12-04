import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.*;

import java.io.File;  
import java.io.FileNotFoundException;  
import java.util.Scanner; 
import static java.util.Map.entry; 
public class trendMapper extends 
      Mapper<LongWritable, Text, Text, MapWritable> {
  private ArrayList<String> arr;
  private int topK;
  private Map<String, String> hashTable = Map.ofEntries(
    entry("AL", "Alabama"), entry("AK", "Alaska"), entry("AZ", "Arizona"),
    entry("AR", "Arkansas"), entry("AS", "American Samoa"), entry("CA", "California"),
    entry("CO", "Colorado"), entry("CT", "Connecticut"), entry("DE", "Delaware"),
    entry("DC", "District of Columbia"), entry("FL", "Florida"), entry("GA", "Georgia"),
    entry("GU", "Guam"), entry("HI", "Hawaii"), entry("ID", "Idaho"),
    entry("IL", "Illinois"), entry("Indiana", "IN"), entry("IA", "Iowa"),
    entry("KS", "Kansas"), entry("KY", "Kentucky"), entry("LA", "Louisiana"),
    entry("ME", "Maine"), entry("MD", "Maryland"), entry("MA", "Massachusetts"),
    entry("MI", "Michigan"), entry("MN", "Minnesota"), entry("MS", "Mississippi"),
    entry("MO", "Missouri"), entry("NE", "Nebraska"), entry("NH", "New Hampshire"),
    entry("NJ", "New Jersey"), entry("NM", "New Mexico"), entry("NY", "New York"),
    entry("NC", "North Carolina"), entry("ND", "North Dakota"), entry("MP", "Nothern Mariana Islands"),
    entry("OH", "Ohio"), entry("OK", "Oklahoma"), entry("OR", "Oregon"),
    entry("PA", "Pennsylvania"), entry("PR", "Puerto Rico"), entry("RI", "Rhode Island"),
    entry("SC", "South Carolina"), entry("SD", "South Dakota"), entry("TN", "Tennessee"),
    entry("TX", "Texas"), entry("TT", "Trust Territories"), entry("UT", "Utah"),
    entry("VT", "Vermont"), entry("VA", "Virginia"), entry("VI", "Virgin Islands"),
    entry("WA", "Washington"), entry("WV", "West Virginia"), entry("WI", "Wiscosin"),
    entry("WY", "Wyoning")
);
  @Override
  public void map(LongWritable key, 
                  Text value, 
                  Context context)
    throws IOException, InterruptedException {
      String parsed = value.toString();
      ArrayList<String> cur = new ArrayList<String>();
      int start=-1;
      for (int end=0; end<=parsed.length(); end++){
        if (end == parsed.length() || parsed.charAt(end) == ','){
          cur.add((start + 1 == parsed.length() ||
            start + 1 == end) ? "-1" :
            parsed.substring(start + 1, end));
          start = end;
        }
      }
      String sub = cur.get(1).substring(0, 2);
      if (cur.get(1).length() < 3 || !sub.contains("US")){return;}
    
      if (isNumeric(cur.get(2))){
        TreeMap<Float, String> hash = new TreeMap<Float, String>();
        for (int i=2; i < cur.size(); i++){
          if (Float.parseFloat(cur.get(i)) != -1){
            hash.put(Float.parseFloat(cur.get(i)), 
                    arr.get(i - 2));
          } 
          if (hash.size() > topK){
            hash.remove(hash.firstKey());
          }
        }
        MapWritable out = new MapWritable();
        for (Map.Entry<Float, String> entry : hash.entrySet()){
          out.put(new Text(entry.getValue()),
                  new FloatWritable(entry.getKey()));
        }
        String state;
        String[] all = cur.get(1).split("_");
        state = hashTable.get(all[1]);
       
        context.write(new Text(cur.get(0)+ "," + state), out);
      }
  }

  public static boolean isNumeric(String str) { 
    try {  
      Float.parseFloat(str);  
      return true;
    } 
    catch(NumberFormatException e){  
      return false;  
    }  
  }

  /*
   * @Override
  public void setup(Context context) throws IOException, InterruptedException {
    arr = new ArrayList<String>();
    String path = context.getConfiguration().get("header");
    topK = Integer.parseInt(context.getConfiguration().get("topK"));
    try {
      File myObj = new File(path);
      String data;
      Scanner myReader = new Scanner(myObj);
      data = myReader.nextLine();
      myReader.close();
      String[] tokens = data.split(","); 
      for (int i=2; i < tokens.length; i++){
        String [] cur = tokens[i].split("_");
        cur = Arrays.copyOfRange(cur, 2, cur.length);
        arr.add(String.join("_", cur));
      }
     
    } 
    catch (FileNotFoundException err) {
      System.out.println("An error occurred.");
      err.printStackTrace();
    }
  }
   */
   
  @Override
  public void setup(Context context) throws IOException, InterruptedException {
    topK = Integer.parseInt(context.getConfiguration().get("topK"));
    arr = new ArrayList<String>(
    Arrays.asList(
      "date","location_key","abdominal_obesity","abdominal_pain","acne","actinic_keratosis","acute_bronchitis","adrenal_crisis",
      "ageusia","alcoholism","allergic_conjunctivitis","allergy","amblyopia","amenorrhea","amnesia","anal_fissure","anaphylaxis",
      "anemia","angina_pectoris","angioedema","angular_cheilitis","anosmia","anxiety","aphasia","aphonia","apnea","arthralgia",
      "arthritis","ascites","asperger_syndrome","asphyxia","asthma","astigmatism","ataxia","atheroma","attention_deficit_hyperactivity_disorder",
      "auditory_hallucination","autoimmune_disease","avoidant_personality_disorder","back_pain","bacterial_vaginosis","balance_disorder",
      "beaus_lines","bells_palsy","biliary_colic","binge_eating","bleeding","bleeding_on_probing","blepharospasm","bloating","blood_in_stool",
      "blurred_vision","blushing","boil","bone_fracture","bone_tumor","bowel_obstruction","bradycardia","braxton_hicks_contractions",
      "breakthrough_bleeding","breast_pain","bronchitis","bruise","bruxism","bunion","burn","burning_chest_pain","burning_mouth_syndrome",
      "candidiasis","canker_sore","cardiac_arrest","carpal_tunnel_syndrome","cataplexy","cataract","chancre","cheilitis","chest_pain",
      "chills","chorea","chronic_pain","cirrhosis","cleft_lip_and_cleft_palate","clouding_of_consciousness","cluster_headache","colitis",
      "coma","common_cold","compulsive_behavior","compulsive_hoarding","confusion","congenital_heart_defect","conjunctivitis","constipation",
      "convulsion","cough","crackles","cramp","crepitus","croup","cyanosis","dandruff","delayed_onset_muscle_soreness","dementia",
      "dentin_hypersensitivity","depersonalization","depression","dermatitis","desquamation","developmental_disability","diabetes",
      "diabetic_ketoacidosis","diarrhea","dizziness","dry_eye_syndrome","dysautonomia","dysgeusia","dysmenorrhea","dyspareunia",
      "dysphagia","dysphoria","dystonia","dysuria","ear_pain","eczema","edema","encephalitis","encephalopathy","epidermoid_cyst",
      "epilepsy","epiphora","erectile_dysfunction","erythema","erythema_chronicum_migrans","esophagitis","excessive_daytime_sleepiness",
      "eye_pain","eye_strain","facial_nerve_paralysis","facial_swelling","fasciculation","fatigue","fatty_liver_disease","fecal_incontinence",
      "fever","fibrillation","fibrocystic_breast_changes","fibromyalgia","flatulence","floater","focal_seizure","folate_deficiency","food_craving",
      "food_intolerance","frequent_urination","gastroesophageal_reflux_disease","gastroparesis","generalized_anxiety_disorder",
      "generalized_tonic–clonic_seizure","genital_wart","gingival_recession","gingivitis","globus_pharyngis","goitre","gout","grandiosity",
      "granuloma","guilt","hair_loss","halitosis","hay_fever","headache","heart_arrhythmia","heart_murmur","heartburn","hematochezia","hematoma",
      "hematuria","hemolysis","hemoptysis","hemorrhoids","hepatic_encephalopathy","hepatitis","hepatotoxicity","hiccup","hip_pain","hives","hot_flash",
      "hydrocephalus","hypercalcaemia","hypercapnia","hypercholesterolemia","hyperemesis_gravidarum","hyperglycemia","hyperhidrosis",
      "hyperkalemia","hyperlipidemia","hypermobility","hyperpigmentation","hypersomnia","hypertension","hyperthermia","hyperthyroidism",
      "hypertriglyceridemia","hypertrophy","hyperventilation","hypocalcaemia","hypochondriasis","hypoglycemia","hypogonadism","hypokalemia","hypomania",
      "hyponatremia","hypotension","hypothyroidism","hypoxemia","hypoxia","impetigo","implantation_bleeding","impulsivity","indigestion",
      "infection","inflammation","inflammatory_bowel_disease","ingrown_hair","insomnia","insulin_resistance","intermenstrual_bleeding","intracranial_pressure",
      "iron_deficiency","irregular_menstruation","itch","jaundice","kidney_failure","kidney_stone","knee_pain","kyphosis",
      "lactose_intolerance","laryngitis","leg_cramps","lesion","leukorrhea","lightheadedness","low_back_pain","low_grade_fever","lymphedema",
      "major_depressive_disorder","malabsorption","male_infertility","manic_disorder","melasma","melena","meningitis","menorrhagia",
      "middle_back_pain","migraine","milium","mitral_insufficiency","mood_disorder","mood_swing","morning_sickness","motion_sickness",
      "mouth_ulcer","muscle_atrophy","muscle_weakness","myalgia","mydriasis","myocardial_infarction","myoclonus","nasal_congestion",
      "nasal_polyp","nausea","neck_mass","neck_pain","neonatal_jaundice","nerve_injury","neuralgia","neutropenia","night_sweats",
      "night_terror","nocturnal_enuresis","nodule","nosebleed","nystagmus","obesity","onychorrhexis","oral_candidiasis","orthostatic_hypotension",
      "osteopenia","osteophyte","osteoporosis","otitis","otitis_externa","otitis_media","pain","palpitations","pancreatitis",
      "panic_attack","papule","paranoia","paresthesia","pelvic_inflammatory_disease","pericarditis","periodontal_disease",
      "periorbital_puffiness","peripheral_neuropathy","perspiration","petechia","phlegm","photodermatitis","photophobia","photopsia",
      "pleural_effusion","pleurisy","pneumonia","podalgia","polycythemia","polydipsia","polyneuropathy","polyuria","poor_posture",
      "post_nasal_drip","postural_orthostatic_tachycardia_syndrome","prediabetes","proteinuria","pruritus_ani","psychosis","ptosis",
      "pulmonary_edema","pulmonary_hypertension","purpura","pus","pyelonephritis","radiculopathy","rectal_pain","rectal_prolapse","red_eye",
      "renal_colic","restless_legs_syndrome","rheum","rhinitis","rhinorrhea","rosacea","round_ligament_pain","rumination","scar","sciatica","scoliosis",
      "seborrheic_dermatitis","self_harm","sensitivity_to_sound","sexual_dysfunction","shallow_breathing","sharp_pain","shivering",
      "shortness_of_breath","shyness","sinusitis","skin_condition","skin_rash","skin_tag","skin_ulcer","sleep_apnea","sleep_deprivation",
      "sleep_disorder","snoring","sore_throat","spasticity","splenomegaly","sputum","stomach_rumble","strabismus","stretch_marks","stridor","stroke",
      "stuttering","subdural_hematoma","suicidal_ideation","swelling","swollen_feet","swollen_lymph_nodes","syncope","tachycardia",
      "tachypnea","telangiectasia","tenderness","testicular_pain","throat_irritation","thrombocytopenia","thyroid_nodule","tic","tinnitus","tonsillitis",
      "toothache","tremor","trichoptilosis","tumor","type_2_diabetes","unconsciousness","underweight","upper_respiratory_tract_infection","urethritis",
      "urinary_incontinence","urinary_tract_infection","urinary_urgency","uterine_contraction","vaginal_bleeding","vaginal_discharge",
      "vaginitis","varicose_veins","vasculitis","ventricular_fibrillation","ventricular_tachycardia","vertigo","viral_pneumonia","visual_acuity","vomiting",
      "wart","water_retention","weakness","weight_gain","wheeze","xeroderma","xerostomia","yawn"
    ));
    
  }
  
  

}