/**
 * Denmark Farm Safety MCP — Data Ingestion Script
 *
 * Sources: Arbejdstilsynet (at.dk), BFA Groent (branchevejledninger),
 *          Videncenter for Arbejdsmiljoe, Bekendtgoerelser.
 *
 * Usage: npm run ingest
 */

import { createDatabase } from '../src/db.js';
import { mkdirSync, writeFileSync } from 'fs';

mkdirSync('data', { recursive: true });
const db = createDatabase('data/database.db');

const now = new Date().toISOString().split('T')[0];

// ---------------------------------------------------------------------------
// 1. SAFETY GUIDANCE
// ---------------------------------------------------------------------------

const safetyGuidance: Array<{
  topic: string;
  machine_type: string | null;
  species: string | null;
  hazards: string;
  control_measures: string;
  legal_requirements: string;
  ppe_required: string;
  regulation_ref: string;
}> = [
  // -- Risk Assessment (APV) --
  {
    topic: 'APV — Arbejdspladsvurdering (obligatorisk)',
    machine_type: null,
    species: null,
    hazards: 'Alle arbejdsmiljorisici: fysiske, kemiske, biologiske, ergonomiske, psykosociale',
    control_measures: 'Kortlaegning af risici, handlingsplan med prioritering, tidsplan for udbedring, opfoelgning. Inddrag medarbejdere og arbejdsmiljoeorganisation.',
    legal_requirements: 'Lovpligtig for alle arbejdsgivere. Skal daekke alle arbejdsforhold. Skal opdateres mindst hvert 3. aar eller ved vaesentlige aendringer i arbejdsforhold, metoder eller materialer.',
    ppe_required: 'Afhaenger af identificerede risici',
    regulation_ref: 'Arbejdsmiljoeloven kap. 4, Bekendtgoerelse nr. 1234 om APV',
  },
  {
    topic: 'APV — Landbrugsspecifikke krav',
    machine_type: null,
    species: null,
    hazards: 'Maskiner, husdyrhaandtering, kemikalier, stoev, stoej, ergonomi, alenearbejde, unge medarbejdere, biologiske agenser',
    control_measures: 'Brug BFA Groent skabeloner til landbrugets APV. Daek saerlige risici: kraftudtag, gyllehaandtering, silogas, stoev fra korn og foder, tunge loeft, alenearbejde i marken.',
    legal_requirements: 'Landbrugets APV skal omfatte alle driftsomraader inkl. mark, stald, vaerksted og lager. BFA Groent har sektorspecifikke skabeloner tilgaengelige.',
    ppe_required: 'Afhaenger af APV-resultater',
    regulation_ref: 'Arbejdsmiljoeloven, BFA Groent branchevejledninger',
  },

  // -- Machinery Safety --
  {
    topic: 'Traktorsikkerhed',
    machine_type: 'Traktor',
    species: null,
    hazards: 'Vaeltning (hyppigste doedsaarsag i landbruget), pakoering, klemning, fald ved af-/paastigning, stoej, vibrationer',
    control_measures: 'CE-maerkning obligatorisk. Styrtoboejle (ROPS) lovpligtig paa alle traktorer. Sikkerhedssele paakraevet ved monteret ROPS. Dagligt eftersyn foer brug. Aldrig spring af/paa en traktor i bevaegelse.',
    legal_requirements: 'Maskindirektivet 2006/42/EF, Bekendtgoerelse om brug af tekniske hjaelpemidler. ROPS obligatorisk. Sikkerhedssele paabudt naar ROPS er monteret.',
    ppe_required: 'Sikkerhedssele, hoerelse\u00advaern ved hoejt stoejniveau, sikkerhedssko',
    regulation_ref: 'AT-vejledning B.1.3, Maskindirektivet 2006/42/EF',
  },
  {
    topic: 'Kraftudtag (PTO) — Afskaeramning',
    machine_type: 'Kraftudtag / PTO',
    species: null,
    hazards: 'Indvikling i roterende dele, amputation, doedsfald. Loest toej, haar eller smykker kan gribes af akslen.',
    control_measures: 'Afskaeramning af kraftudtag obligatorisk. Afskaeramningen skal daekke hele akslen inklusive universalled. Sluk altid maskine og vent paa fuldstaendigt stop foer vedligehold. Arbejdstoej maa ikke kunne gribes — ingen loese aermer, slips eller kaeder.',
    legal_requirements: 'Afskaeramning paabudt ifoelge Bekendtgoerelse om brug af tekniske hjaelpemidler. Leverandoer skal levere maskine med afskaeramning. Bruger skal sikre at afskaeramning er intakt.',
    ppe_required: 'Taetsiddende arbejdstoej, sikkerhedssko, ingen loese dele',
    regulation_ref: 'AT-vejledning B.1.3, Maskindirektivet bilag I',
  },
  {
    topic: 'Mejetaerskersikkerhed',
    machine_type: 'Mejetaersker',
    species: null,
    hazards: 'Indvikling i skaerevaerk, klemning, fald fra maskine, brand i toert afgroede, stoeveksplosion',
    control_measures: 'Sikkerhedsstop paa alle bevaegelige dele. Rengoeiring og vedligehold kun ved slukket maskine med noegle fjernet. Brandslukker altid medbragt. Fjern ophobet stoev regelmaessigt.',
    legal_requirements: 'CE-maerkning, brugsanvisning paa dansk, dagligt eftersyn, brandslukker paabudt',
    ppe_required: 'Hoerelsevaern, stoevmaske ved rengoeiring, sikkerhedssko, brandslukker',
    regulation_ref: 'AT-vejledning B.1.3, Maskindirektivet 2006/42/EF',
  },
  {
    topic: 'Halmhaandtering — rundballer',
    machine_type: 'Rundballepresser / palaessergrab',
    species: null,
    hazards: 'Fastklemning ved haandtering af rundballer, vaeltende baller, klemning mellem baller og maskine',
    control_measures: 'Brug palaessergrab — aldrig haender. Max stabling 3 baller hoejt. Sikre stabilt underlag. Hold afstand naar baller loeftes. Boern maa ikke opholde sig i naerheden.',
    legal_requirements: 'Korrekt udstyr til haandtering paabudt. Stablingsggraenser skal overholdes.',
    ppe_required: 'Sikkerhedssko, handsker, hjelm ved stablings\u00adarbejde',
    regulation_ref: 'AT-vejledning B.1.3, BFA Groent vejledning om halmhaandtering',
  },

  // -- Chemical Safety --
  {
    topic: 'Bekaempelsesmidler — sproejtearbejde',
    machine_type: 'Marksproeite',
    species: null,
    hazards: 'Forgiftning ved indaanding, hudkontakt eller indtagelse. Kroniske skader ved langvarig eksponering. Miljoeskader.',
    control_measures: 'MAL-faktor (Miljoestyrelsens Arbejdshygiejniske Luftkrav) bestemmer noedvendig beskyttelse. Sproejtecertifikat obligatorisk for alle der haandterer bekaempelsesmidler professionelt. Brug altid korrekte vaernemidler. Overhold re-entry-perioder.',
    legal_requirements: 'Sproejtecertifikat paabudt (fornyelse hvert 4. aar). Bekaempelsesmidler skal vaere godkendt af Miljoestyrelsen. Sproejtejournal obligatorisk. MAL-kodering bestemmer vaernemiddelkrav.',
    ppe_required: 'Handsker (nitril), overtraeksdragt, ansigts\u00adskaerm eller aandedraetsvaern afhaengigt af MAL-faktor, stoevler',
    regulation_ref: 'Bekendtgoerelse om bekaempelsesmidler, AT-vejledning A.1.2',
  },
  {
    topic: 'Ammoniak — stald og gyllehaandtering',
    machine_type: null,
    species: 'Kvaeag, svin',
    hazards: 'Frigivet ved haandtering af goedning og gylle. Graensevaerdi 25 ppm (korttid 50 ppm). Akut livsfarligt over 300 ppm. AEtsning af oejne og luftveje.',
    control_measures: 'Ventilation i stalde er foerste prioritet. Personlige vaernemidler ved gylleomroering. Maal ammoniakkoncentration foer arbejde i lukkede rum. Aldrig gaa ned i gyllebeholder uden redningsudstyr og vagtperson.',
    legal_requirements: 'Graensevaerdi 25 ppm (8-timers), 50 ppm (15-minutters korttidsvaerdi). Ventilationskrav i stalde. Risikovurdering paabudt ved gyllehaandtering.',
    ppe_required: 'Aandedraetsvaern med ammoniakfilter (type K), beskyttelses\u00adbriller, overtraeksdragt, gummistoevler',
    regulation_ref: 'AT-vejledning A.1.2, Graensevaerdibekendtgoerelsen',
  },
  {
    topic: 'Silogas (NO2) — ensilering',
    machine_type: 'Silo',
    species: null,
    hazards: 'NO2 dannes under ensilering. LIVSFARLIGT i lave koncentrationer. Usynlig gas (kan have svag roedbrun farve). Lugtfri i farlige koncentrationer. Forsinket lungeoedem (symptomer kan komme 4-24 timer efter eksponering).',
    control_measures: 'Ventilation minimum 48 timer efter fyldning. Aldrig gaa ind i silo alene. Gasmaske med korrekt filter (type NO-P3) kraevet. Adgang forbudt de foerste 3 uger efter fyldning uden gasmaaaling. Opslaa advarselsskilte.',
    legal_requirements: 'Risikovurdering obligatorisk. Adgangsrestriktioner skal haandhaeves. Redningsudstyr skal vaere tilgaengeligt. Foerstehjaelp med iltudstyr paabudt tilgaengeligt.',
    ppe_required: 'Gasmaske med NO-P3 filter, sikkerhedssele med line, aandedraetsvaern (uafhaengigt ved hoeje koncentrationer)',
    regulation_ref: 'AT-vejledning D.5.5 (Siloer), AT-vejledning A.1.2',
  },

  // -- Livestock --
  {
    topic: 'Husdyrhaandtering — kvaeg',
    machine_type: null,
    species: 'Kvaeg',
    hazards: 'Spark, stanging, klemning mod vaeg/inventar, tramping. Saerlig risiko ved kvaeg med kalve, tyre og nye/urolige dyr.',
    control_measures: 'Brug fanghekke og behandlingsbokse. Aldrig staa bag et dyr i trang passage. Hold flugtrute aaben. Undgaa at arbejde alene med farlige dyr. Undgaa plodselige bevaegelser og stoej.',
    legal_requirements: 'Risikovurdering for dyrehaandtering paabudt. Tyre over 11 maaneder skal have naesring. Inventar skal vaere vedligeholdt og dimensioneret til dyretypen.',
    ppe_required: 'Sikkerhedssko med staaltaa, handsker, evt. hjelm ved behandling',
    regulation_ref: 'Bekendtgoerelse om arbejde med dyr, BFA Groent branchevejledning',
  },
  {
    topic: 'Husdyrhaandtering — svin',
    machine_type: null,
    species: 'Svin',
    hazards: 'Bid, klemning, allergener (stoev, hudfnug), zoonoser (MRSA, salmonella), ammoniakeksponering i stald',
    control_measures: 'Brug sorterings\u00adbretter og drivvaege. God ventilation i stalde. Hygiejneprotokol ved ind-/udgang (sluse). Vaernemidler ved kontakt med syge dyr.',
    legal_requirements: 'Risikovurdering paabudt. Ventilationskrav. MRSA-screening anbefalet for svinebedrift-ansatte.',
    ppe_required: 'Stoevmaske (FFP2), handsker, overtraeksdragt, stoevler, hoerelsevaern',
    regulation_ref: 'Bekendtgoerelse om arbejde med dyr, BFA Groent',
  },

  // -- Stald og faciliteter --
  {
    topic: 'Staldindretning — arbejdsmiljoekrav',
    machine_type: null,
    species: 'Kvaeg, svin, fjaerkrae',
    hazards: 'Fald paa vaade/glatte gulve, stoev, ammoniak, stoej, ergonomiske belastninger, biologiske agenser',
    control_measures: 'Skridsikre gulve, tilstraekkelig belysning (min. 200 lux i arbejdsomraader), mekanisk ventilation, ryddelighed, vedligehold af inventar.',
    legal_requirements: 'AT-vejledning D.2.20 fastsaetter krav til staldindretning: lofthoejde min. 2,1 m i gangarealer, ventilation, belysning, adgangsforhold, flugtveje.',
    ppe_required: 'Sikkerhedssko, hoerelsevaern, stoevmaske, handsker afhaengigt af dyretype',
    regulation_ref: 'AT-vejledning D.2.20 (Indretning af stalde)',
  },

  // -- Ergonomi og alenearbejde --
  {
    topic: 'Ergonomi — tunge loeft i landbruget',
    machine_type: null,
    species: null,
    hazards: 'Rygskader, muskel-skeletbelastninger ved gentagne tunge loeft, akavede arbejdsstillinger',
    control_measures: 'Brug mekaniske hjaelpemidler (loefteborde, saekkeloeftere, transportbaand). Max loeftevaegt 25 kg taet paa kroppen. Varieer arbejdsstillinger. Pauser ved gentagne loeft.',
    legal_requirements: 'Bekendtgoerelse om manuel haandtering. Risikovurdering af loeftearbejde paabudt. Mekaniske hjaelpemidler skal stilles til raadighed naar det er muligt.',
    ppe_required: 'Sikkerhedssko, handsker, evt. rygstoette ved saerlige opgaver',
    regulation_ref: 'AT-vejledning D.3.1 (Loeft, traek og skub)',
  },
  {
    topic: 'Alenearbejde',
    machine_type: null,
    species: null,
    hazards: 'Forsinket hjaelp ved ulykker, isolation, psykisk belastning, ingen overvaakning',
    control_measures: 'Alenearbejdspolitik med klare procedurer. Mobiltelefon eller personsoegeapparat. Regelmaessig tjek-ind-ordning. Tilkaldemulighed (mandown-alarm). Klare instruktioner for noedprocedurer.',
    legal_requirements: 'Alenearbejde skal indgaa i APV. Visse opgaver maa ikke udfoeres alene (gylleomroering, siloarbejde, arbejde med farlige dyr).',
    ppe_required: 'Mobiltelefon/personsoegeapparat, foerstehjaelpskasse, egnet vaernemiddel til opgaven',
    regulation_ref: 'Arbejdsmiljoeloven, BFA Groent vejledning om alenearbejde',
  },
];

const insertGuidance = db.instance.prepare(`
  INSERT INTO safety_guidance (topic, machine_type, species, hazards, control_measures, legal_requirements, ppe_required, regulation_ref)
  VALUES (?, ?, ?, ?, ?, ?, ?, ?)
`);

for (const g of safetyGuidance) {
  insertGuidance.run(g.topic, g.machine_type, g.species, g.hazards, g.control_measures, g.legal_requirements, g.ppe_required, g.regulation_ref);
}

console.log(`Inserted ${safetyGuidance.length} safety guidance records.`);

// ---------------------------------------------------------------------------
// 2. CHILDREN / YOUNG WORKERS RULES
// ---------------------------------------------------------------------------

const childrenRules: Array<{
  age_group: string;
  activity: string;
  permitted: number;
  conditions: string | null;
  regulation_ref: string;
}> = [
  {
    age_group: 'Under 13',
    activity: 'Alt arbejde',
    permitted: 0,
    conditions: 'Intet arbejde tilladt for boern under 13 aar.',
    regulation_ref: 'Bekendtgoerelse om unges arbejde',
  },
  {
    age_group: '13-14',
    activity: 'Let ikke-farligt arbejde',
    permitted: 1,
    conditions: 'Kun let, ikke-farligt arbejde: baerplukning, rengoeiring, lettere fodringsopgaver. Max 2 timer paa skoledage, 7 timer i ferier. Ikke mellem kl. 20-06.',
    regulation_ref: 'Bekendtgoerelse om unges arbejde, kap. 3',
  },
  {
    age_group: '13-14',
    activity: 'Traktorkoeorsel',
    permitted: 0,
    conditions: 'Forbudt for 13-14-aarige uanset omstaendigheder.',
    regulation_ref: 'Bekendtgoerelse om unges arbejde',
  },
  {
    age_group: '13-14',
    activity: 'Arbejde med kemikalier',
    permitted: 0,
    conditions: 'Forbudt. Ingen kontakt med bekaempelsesmidler, goedning eller andre kemikalier.',
    regulation_ref: 'Bekendtgoerelse om unges arbejde',
  },
  {
    age_group: '15-17',
    activity: 'Normal arbejdstid — ikke-farligt arbejde',
    permitted: 1,
    conditions: 'Max 8 timer pr. dag, 40 timer pr. uge. Ikke mellem kl. 20-06. Ingen overarbejde. 30 min pause ved 4,5 timers arbejde.',
    regulation_ref: 'Bekendtgoerelse om unges arbejde, kap. 5',
  },
  {
    age_group: '15-17',
    activity: 'Traktorkoeorsel',
    permitted: 1,
    conditions: 'Kun for 16-17-aarige med gyldigt traktorkoerekort (kategori TM). Kun paa bedriftens omraade og offentlig vej med max 30 km/t. Ikke i naerhed af andre medarbejdere til fods.',
    regulation_ref: 'Bekendtgoerelse om unges arbejde, Faerdselsloven',
  },
  {
    age_group: '15-17',
    activity: 'Arbejde med kraftudtag (PTO)',
    permitted: 0,
    conditions: 'Forbudt for alle under 18 aar. Kraftudtag er klassificeret som saerligt farligt teknisk hjaelpemiddel.',
    regulation_ref: 'Bekendtgoerelse om unges arbejde, bilag 1',
  },
  {
    age_group: '15-17',
    activity: 'Arbejde med kemikalier / bekaempelsesmidler',
    permitted: 0,
    conditions: 'Forbudt for alle under 18 aar. Omfatter blanding, udkoersel og kontakt med behandlede afgroeder inden re-entry-perioden.',
    regulation_ref: 'Bekendtgoerelse om unges arbejde, bilag 1',
  },
  {
    age_group: '15-17',
    activity: 'Arbejde med farlige dyr',
    permitted: 0,
    conditions: 'Forbudt for alle under 18 aar. Omfatter tyre over 11 maaneder, soeer med grise, aggressive dyr.',
    regulation_ref: 'Bekendtgoerelse om unges arbejde, bilag 1',
  },
  {
    age_group: '15-17',
    activity: 'Arbejde i hoejden over 2 meter',
    permitted: 0,
    conditions: 'Forbudt for alle under 18 aar. Omfatter arbejde paa tage, siloer, foderanlaeag.',
    regulation_ref: 'Bekendtgoerelse om unges arbejde, bilag 1',
  },
];

const insertChild = db.instance.prepare(`
  INSERT INTO children_rules (age_group, activity, permitted, conditions, regulation_ref)
  VALUES (?, ?, ?, ?, ?)
`);

for (const c of childrenRules) {
  insertChild.run(c.age_group, c.activity, c.permitted, c.conditions, c.regulation_ref);
}

console.log(`Inserted ${childrenRules.length} children/young worker rules.`);

// ---------------------------------------------------------------------------
// 3. REPORTING REQUIREMENTS (anmeldelse af arbejdsulykker)
// ---------------------------------------------------------------------------

const reportingReqs: Array<{
  incident_type: string;
  reportable: number;
  deadline: string;
  notify: string;
  method: string;
  record_retention_years: number;
  regulation_ref: string;
}> = [
  {
    incident_type: 'Arbejdsulykke med fravaaer over 1 dag',
    reportable: 1,
    deadline: '9 dage fra ulykkesdagen',
    notify: 'Arbejdstilsynet',
    method: 'Elektronisk via EASY-systemet (easy.telefonkommunen.dk). Arbejdsgiver har anmeldepligt.',
    record_retention_years: 5,
    regulation_ref: 'Bekendtgoerelse om anmeldelse af arbejdsulykker, kap. 2',
  },
  {
    incident_type: 'Alvorlig ulykke (knoglebrud, amputation, bevidstloeshed)',
    reportable: 1,
    deadline: 'STRAKS — telefonisk anmeldelse',
    notify: 'Arbejdstilsynet via telefon 70 12 12 88',
    method: 'Telefonisk straks, efterfulgt af skriftlig anmeldelse via EASY inden 9 dage. Ulykkesstedet maa ikke aendres foer Arbejdstilsynet har givet tilladelse.',
    record_retention_years: 10,
    regulation_ref: 'Bekendtgoerelse om anmeldelse af arbejdsulykker, kap. 3',
  },
  {
    incident_type: 'Doedsfald paa arbejdspladsen',
    reportable: 1,
    deadline: 'STRAKS — telefonisk anmeldelse',
    notify: 'Arbejdstilsynet (70 12 12 88) og politiet (114). Politiet underrettes automatisk ved doedsfald.',
    method: 'Telefonisk straks. Ulykkesstedet maa IKKE forstyrres. Politiet sikrer gerningsstedet. Arbejdstilsynet foretager ulykkesundersoeagelse.',
    record_retention_years: 10,
    regulation_ref: 'Bekendtgoerelse om anmeldelse af arbejdsulykker, kap. 3, Straffeloven',
  },
  {
    incident_type: 'Naerved-ulykke (ingen skade, men potentielt farlig haendelse)',
    reportable: 0,
    deadline: 'Ingen lovpligtig frist',
    notify: 'Intern registrering i virksomhedens arbejdsmiljoesystem',
    method: 'Intern registrering. Anbefales kraftigt som del af forebyggelsesarbejdet. Behandles i arbejdsmiljoeoorganisationen.',
    record_retention_years: 5,
    regulation_ref: 'Anbefaling fra Arbejdstilsynet og BFA Groent',
  },
  {
    incident_type: 'Erhvervssygdom (stoev, kemikalier, belastningslidelser)',
    reportable: 1,
    deadline: 'Snarest muligt — laegen har anmeldepligt',
    notify: 'Arbejdstilsynet og Arbejdsmarkedets Erhvervssikring (AES)',
    method: 'Laegen anmelder via EASY. Arbejdsgiver underrettes. Registreres ogsaa i APV.',
    record_retention_years: 10,
    regulation_ref: 'Bekendtgoerelse om anmeldelse af erhvervssygdomme',
  },
];

const insertReport = db.instance.prepare(`
  INSERT INTO reporting_requirements (incident_type, reportable, deadline, notify, method, record_retention_years, regulation_ref)
  VALUES (?, ?, ?, ?, ?, ?, ?)
`);

for (const r of reportingReqs) {
  insertReport.run(r.incident_type, r.reportable, r.deadline, r.notify, r.method, r.record_retention_years, r.regulation_ref);
}

console.log(`Inserted ${reportingReqs.length} reporting requirements.`);

// ---------------------------------------------------------------------------
// 4. COSHH GUIDANCE (kemisk arbejdsmiljoe)
// ---------------------------------------------------------------------------

const coshhGuidance: Array<{
  substance_type: string;
  activity: string;
  assessment_required: number;
  ppe: string;
  storage_requirements: string;
  disposal_requirements: string;
  regulation_ref: string;
}> = [
  {
    substance_type: 'Bekaempelsesmidler (pesticider, herbicider, fungicider)',
    activity: 'Sproejtearbejde, blanding, paafyldning, rengoeiring af udstyr',
    assessment_required: 1,
    ppe: 'Nitrilhandsker, overtraeksdragt (type 4/5/6 afhaengigt af MAL-faktor), ansigtsvaern eller aandedraetsvaern (A2P3), stoevler. Se produktets sikkerhedsdatablad.',
    storage_requirements: 'Aflaaset kemikalierum med ventilation, opsamlingskar, adskilt fra foder/fodevarer, skiltning. Sikkerhedsdatablade tilgaengelige.',
    disposal_requirements: 'Emballage skylles 3 gange og afleveres paa kommunal genbrugsstation. Restmidler afleveres som farligt affald. Sproejtevand maa ikke udledes i draen.',
    regulation_ref: 'AT-vejledning A.1.2, Bekendtgoerelse om bekaempelsesmidler, Miljoebeskyttelsesloven',
  },
  {
    substance_type: 'Ammoniak (fra goedning, gylle, staldluft)',
    activity: 'Gylleomroering, gylleudkoersel, staldarbejde, rengoering af gyllebeholder',
    assessment_required: 1,
    ppe: 'Aandedraetsvaern med ammoniakfilter (type K2P3), beskyttelsesbriller, kemikaliedraggt, gummistoevler',
    storage_requirements: 'Gyllebeholdere skal vaere overdaekket. Sikkerhedsafstand til beboelse. Alarmsystem ved indendoers anlaeag.',
    disposal_requirements: 'Udkoersel ifoelge goedningsregnskab. Nedfaeldning paabudt paa ubevokset jord. Udbringningsperioder reguleret.',
    regulation_ref: 'AT-vejledning A.1.2, Graensevaerdibekendtgoerelsen, Husdyrgoedningsbekendtgoerelsen',
  },
  {
    substance_type: 'Silogas (NO2, kvaeelstofoxider)',
    activity: 'Ensilering, adgang til silo efter fyldning',
    assessment_required: 1,
    ppe: 'Uafhaengigt aandedraetsvaern (trykluft) ved adgang til silo inden 3 uger efter fyldning. Gasmaske med NO-P3 filter derefter. Sikkerhedssele med line og vagtperson.',
    storage_requirements: 'Ventilation paabudt minimum 48 timer efter fyldning. Advarselsskilte opsat ved alle adgange. Gasdetektor tilgaengelig.',
    disposal_requirements: 'Ikke relevant — gas spredes naturligt. Ventilation sikrer sikker koncentration.',
    regulation_ref: 'AT-vejledning D.5.5 (Siloer), AT-vejledning A.1.2',
  },
  {
    substance_type: 'Kornstoeov og foderstoeov',
    activity: 'Korntoeorring, formaling, fodring, rengoeiring af kornanlaeag',
    assessment_required: 1,
    ppe: 'Stoevmaske FFP2/FFP3, beskyttelsesbriller, overtraeksdragt ved kraftig stoevudvikling',
    storage_requirements: 'Lukkede systemer foretrukket. God ventilation. Eksplosionsforebyggelse ved koncentreret kornstoeov.',
    disposal_requirements: 'Regelmaessig rengoeiring for at undgaa stoevophobning. Stoevsamlere skal tooemmes og vedligeholdes.',
    regulation_ref: 'AT-vejledning A.1.2, Bekendtgoerelse om stoevgraeensevaerdier, ATEX-direktiver',
  },
  {
    substance_type: 'Dieselbraendstof og smoeremidler',
    activity: 'Paafyldning af maskiner, vedligehold, olieshift',
    assessment_required: 1,
    ppe: 'Nitrilhandsker, sikkerhedsbriller, overtraeksdragt ved risiko for spaenk',
    storage_requirements: 'Dobbeltvaegget tank eller opsamlingskar (110% kapacitet). Aflaaset. Brandslukkningsudstyr. Min. 5 meter fra bygninger.',
    disposal_requirements: 'Spildolie opsamles og afleveres som farligt affald. Oliefiltre er farligt affald. Aldrig haeld olie i kloak eller jord.',
    regulation_ref: 'AT-vejledning A.1.2, Miljoebeskyttelsesloven, Olietankbekendtgoerelsen',
  },
];

const insertCoshh = db.instance.prepare(`
  INSERT INTO coshh_guidance (substance_type, activity, assessment_required, ppe, storage_requirements, disposal_requirements, regulation_ref)
  VALUES (?, ?, ?, ?, ?, ?, ?)
`);

for (const c of coshhGuidance) {
  insertCoshh.run(c.substance_type, c.activity, c.assessment_required, c.ppe, c.storage_requirements, c.disposal_requirements, c.regulation_ref);
}

console.log(`Inserted ${coshhGuidance.length} COSHH/chemical safety records.`);

// ---------------------------------------------------------------------------
// 5. RISK ASSESSMENT TEMPLATES
// ---------------------------------------------------------------------------

const riskTemplates: Array<{
  activity: string;
  hazards: string;
  controls: string;
  residual_risk: string;
  review_frequency: string;
}> = [
  {
    activity: 'Traktorkoeorsel — marktransport',
    hazards: 'Vaeltning, paakoeorsel, fald ved af-/paastigning, stoej, vibrationer, helkropsvibrationer',
    controls: 'ROPS monteret, sikkerhedssele, dagligt eftersyn, stabile koereflader, max hastighed afpasset terreen, hoerelsevaern',
    residual_risk: 'Lav — under forudsaetning af korrekt brug af ROPS og sele',
    review_frequency: 'Aarligt og ved nye maskiner eller aeandrede forhold',
  },
  {
    activity: 'Gylleomroering og -udkoeorsel',
    hazards: 'Ammoniakforgiftning, kvaelning (iltmangel), drukning, eksplosion (metan/svovlbrinte), fald i aaben gyllebeholder',
    controls: 'Aldrig alenearbejde. Gasmaaler paabudt. Aandedraetsvaern tilgaengeligt. Omroering kun udendoers/med ventilation. Indhegning af aabne beholdere. Noedberedskab med livline.',
    residual_risk: 'Middel — selv med kontrolforanstaltninger er gyllehaandtering hoejrisiko',
    review_frequency: 'Foer hver gyllesaeson og ved aendringer i anlaeag',
  },
  {
    activity: 'Haandtering af kvaeg (fiksering, behandling, laesning)',
    hazards: 'Spark, stanging, klemning, tramping, zoonoser (ringorm, leptospirose)',
    controls: 'Fanghekke, behandlingsboks, flugtruter, to-personsregel ved farlige dyr, naesring paa tyre over 11 mdr., rolig haandtering',
    residual_risk: 'Middel — dyreadfaerd er uforudsigelig',
    review_frequency: 'Halvaarligt og ved nye dyr/aeandret besaetning',
  },
  {
    activity: 'Siloarbejde — fyldning og tooemning',
    hazards: 'Silogas (NO2), iltmangel, fald, klemning i udtoeomningsudstyr, stoev',
    controls: 'Ventilation min. 48 timer. Advarselsskilte. Gasmaaler foer adgang. Uafhaengigt aandedraetsvaern. Sikkerhedssele og vagtperson. Aldrig gaa ind alene.',
    residual_risk: 'Hoej — potentielt doedeligt, reduceres men elimineres ikke med kontroller',
    review_frequency: 'Foer hver silo\u00adsaeson og ved aeandringer',
  },
  {
    activity: 'Sproejtearbejde — bekaempelsesmidler',
    hazards: 'Forgiftning (akut/kronisk), hudaetsning, oejenskader, miljoeskader',
    controls: 'Sproejtecertifikat, korrekte vaernemidler ifoelge MAL-faktor, sikkerhedsdatablad laest, vindretning vurderet, re-entry-perioder overholdt, rengoering af udstyr efter brug',
    residual_risk: 'Lav til middel — afhaengigt af middlets faarlighed (MAL-faktor)',
    review_frequency: 'Aarligt, ved nye midler og ved aeandrede MAL-vurderinger',
  },
  {
    activity: 'Kornhaandtering og -toerring',
    hazards: 'Stoeveksplosion, indfangning i kornmasse, stoeveksponering (luftveje), stoej fra ventilatorer',
    controls: 'Lukket system, eksplosionsforebyggende foranstaltninger (ATEX), FFP2/3-maske, hoerelsevaern, aldrig gaa ind i kornbeholder mens udtooemning foregaar',
    residual_risk: 'Middel — eksplosionsrisiko kstraever konstant opmaerksomhed',
    review_frequency: 'Aarligt og ved aeandringer i anlaeag eller afgroeder',
  },
  {
    activity: 'Vedligehold af maskiner — vaeerkstedsarbejde',
    hazards: 'Klemning, skaering, braendskader, kemikalier (smoeremidler, hydraulikvaeske), fald fra maskine, tunge loeft',
    controls: 'Maskine slukket og sikret (lockout/tagout), korrekt vaerktoeaj, mekaniske loeftemidler, ventilation ved svejsning, foerstehjlaelpskasse',
    residual_risk: 'Lav — med korrekt lockout/tagout procedure',
    review_frequency: 'Aarligt',
  },
];

const insertTemplate = db.instance.prepare(`
  INSERT INTO risk_assessment_templates (activity, hazards, controls, residual_risk, review_frequency)
  VALUES (?, ?, ?, ?, ?)
`);

for (const t of riskTemplates) {
  insertTemplate.run(t.activity, t.hazards, t.controls, t.residual_risk, t.review_frequency);
}

console.log(`Inserted ${riskTemplates.length} risk assessment templates.`);

// ---------------------------------------------------------------------------
// 6. FTS5 SEARCH INDEX
// ---------------------------------------------------------------------------

// Clear existing FTS index
db.instance.exec('DELETE FROM search_index');

// Index safety guidance
for (const g of safetyGuidance) {
  db.instance.prepare(
    'INSERT INTO search_index (title, body, topic, jurisdiction) VALUES (?, ?, ?, ?)'
  ).run(
    g.topic,
    [g.hazards, g.control_measures, g.legal_requirements, g.ppe_required, g.regulation_ref].join(' '),
    'safety_guidance',
    'DK'
  );
}

// Index children rules
for (const c of childrenRules) {
  db.instance.prepare(
    'INSERT INTO search_index (title, body, topic, jurisdiction) VALUES (?, ?, ?, ?)'
  ).run(
    `${c.age_group}: ${c.activity}`,
    [c.conditions, c.regulation_ref].filter(Boolean).join(' '),
    'children_rules',
    'DK'
  );
}

// Index reporting requirements
for (const r of reportingReqs) {
  db.instance.prepare(
    'INSERT INTO search_index (title, body, topic, jurisdiction) VALUES (?, ?, ?, ?)'
  ).run(
    r.incident_type,
    [r.deadline, r.notify, r.method, r.regulation_ref].join(' '),
    'reporting_requirements',
    'DK'
  );
}

// Index COSHH guidance
for (const c of coshhGuidance) {
  db.instance.prepare(
    'INSERT INTO search_index (title, body, topic, jurisdiction) VALUES (?, ?, ?, ?)'
  ).run(
    `${c.substance_type} — ${c.activity}`,
    [c.ppe, c.storage_requirements, c.disposal_requirements, c.regulation_ref].join(' '),
    'coshh_guidance',
    'DK'
  );
}

// Index risk assessment templates
for (const t of riskTemplates) {
  db.instance.prepare(
    'INSERT INTO search_index (title, body, topic, jurisdiction) VALUES (?, ?, ?, ?)'
  ).run(
    t.activity,
    [t.hazards, t.controls, t.residual_risk, t.regulation_ref ?? ''].join(' '),
    'risk_assessment_templates',
    'DK'
  );
}

// -- Additional AT-Vejledninger as search index entries --
const atVejledninger = [
  {
    title: 'AT-vejledning A.1.2 — Arbejde med stoffer og materialer',
    body: 'Kemisk arbejdsmiljoe. Regler for haandtering, opbevaring og bortskaffelse af kemiske stoffer paa arbejdspladsen. Omfatter bekaempelsesmidler, rengoeringsmidler, braendstoffer, oplosningsemidle i landbruget. MAL-faktor, graensevaerdier, substitution, ventilation, personlige vaernemidler.',
  },
  {
    title: 'AT-vejledning B.1.3 — Maskiner og maskinanlaeg',
    body: 'Maskinsikkerhed. Krav til CE-maerkning, afskaeramning, noedstop, vedligehold og instruktion. Galder traktorer, mejetaerskere, kraftudtag, snittevaerker, transportudstyr og oevrigt landbrugsmaskineri. Lockout/tagout procedurer.',
  },
  {
    title: 'AT-vejledning D.2.20 — Indretning af stalde',
    body: 'Staldindretning og arbejdsmiljoe. Krav til lofthoejde, ventilation, belysning, gulve, gangarealer, flugtveje. Galder kvaeg-, svine- og fjaerkraestalde. Ergonomiske krav til arbejdspladser i stalde.',
  },
  {
    title: 'AT-vejledning D.5.5 — Siloer',
    body: 'Sikkerhed ved siloer. Farerne ved silogas (NO2). Ventilationskrav, adgangsbeggraensninger, gasmaaling, personlige vaernemidler, redningsudstyr. Gaelder plansiloer, taarnssiloer og gastatte siloer.',
  },
];

for (const v of atVejledninger) {
  db.instance.prepare(
    'INSERT INTO search_index (title, body, topic, jurisdiction) VALUES (?, ?, ?, ?)'
  ).run(v.title, v.body, 'at_vejledning', 'DK');
}

// -- Kronesmiley system --
const kronesmiley = [
  {
    title: 'Kronesmiley — Groen (godt arbejdsmiljoe)',
    body: 'Groen kronesmiley: Ingen problemer konstateret ved Arbejdstilsynets tilsyn. Godt arbejdsmiljoe. Virksomheden overholder alle krav. Giltig i 5 aar.',
  },
  {
    title: 'Kronesmiley — Gul (mindre problemer)',
    body: 'Gul kronesmiley: Mindre problemer konstateret. Paabud om forbedring udstedt. Virksomheden skal udbedre mangler inden frist. Opfoelgningstilsyn kan forekomme.',
  },
  {
    title: 'Kronesmiley — Roed (alvorlige problemer)',
    body: 'Roed kronesmiley: Vaesentlige eller alvorlige problemer. Strakspaabeud eller forbud udstedt. Overhaaengende fare for sikkerhed eller sundhed. Aktiviteten kan vaere standset. Opfoelgningstilsyn sker altid.',
  },
];

for (const k of kronesmiley) {
  db.instance.prepare(
    'INSERT INTO search_index (title, body, topic, jurisdiction) VALUES (?, ?, ?, ?)'
  ).run(k.title, k.body, 'kronesmiley', 'DK');
}

const totalFts = safetyGuidance.length + childrenRules.length + reportingReqs.length + coshhGuidance.length + riskTemplates.length + atVejledninger.length + kronesmiley.length;
console.log(`Built FTS5 search index with ${totalFts} entries.`);

// ---------------------------------------------------------------------------
// 7. METADATA
// ---------------------------------------------------------------------------

db.run("INSERT OR REPLACE INTO db_metadata (key, value) VALUES ('mcp_name', 'Danish Farm Safety MCP')");
db.run("INSERT OR REPLACE INTO db_metadata (key, value) VALUES ('jurisdiction', 'DK')");
db.run("INSERT OR REPLACE INTO db_metadata (key, value) VALUES ('schema_version', '1.0')");
db.run("INSERT OR REPLACE INTO db_metadata (key, value) VALUES ('last_ingest', ?)", [now]);
db.run("INSERT OR REPLACE INTO db_metadata (key, value) VALUES ('build_date', ?)", [now]);
db.run("INSERT OR REPLACE INTO db_metadata (key, value) VALUES ('data_sources', 'Arbejdstilsynet (at.dk), BFA Groent (branchevejledninger), Videncenter for Arbejdsmiljoe')");
db.run("INSERT OR REPLACE INTO db_metadata (key, value) VALUES ('disclaimer', 'Data er vejledende. Kontakt Arbejdstilsynet for aktuelle regler og vejledninger.')");
db.run("INSERT OR REPLACE INTO db_metadata (key, value) VALUES ('record_count', ?)", [
  String(safetyGuidance.length + childrenRules.length + reportingReqs.length + coshhGuidance.length + riskTemplates.length),
]);

// ---------------------------------------------------------------------------
// 8. COVERAGE FILE
// ---------------------------------------------------------------------------

writeFileSync('data/coverage.json', JSON.stringify({
  mcp_name: 'Danish Farm Safety MCP',
  jurisdiction: 'DK',
  build_date: now,
  status: 'populated',
  record_counts: {
    safety_guidance: safetyGuidance.length,
    children_rules: childrenRules.length,
    reporting_requirements: reportingReqs.length,
    coshh_guidance: coshhGuidance.length,
    risk_assessment_templates: riskTemplates.length,
    fts_index_entries: totalFts,
  },
  data_sources: [
    'Arbejdstilsynet (at.dk)',
    'BFA Groent (branchevejledninger)',
    'Videncenter for Arbejdsmiljoe',
  ],
  key_regulations: [
    'Arbejdsmiljoeloven',
    'Bekendtgoerelse om unges arbejde',
    'Bekendtgoerelse om anmeldelse af arbejdsulykker',
    'Bekendtgoerelse om bekaempelsesmidler',
    'AT-vejledning A.1.2 (Stoffer og materialer)',
    'AT-vejledning B.1.3 (Maskiner og maskinanlaeg)',
    'AT-vejledning D.2.20 (Indretning af stalde)',
    'AT-vejledning D.5.5 (Siloer)',
    'Maskindirektivet 2006/42/EF',
  ],
  disclaimer: 'Data er vejledende. Kontakt Arbejdstilsynet for aktuelle regler og vejledninger.',
}, null, 2));

db.close();

console.log('');
console.log('=== Danish Farm Safety MCP — ingestion complete ===');
console.log(`  Safety guidance:         ${safetyGuidance.length} records`);
console.log(`  Children/young workers:  ${childrenRules.length} records`);
console.log(`  Reporting requirements:  ${reportingReqs.length} records`);
console.log(`  Chemical safety (COSHH): ${coshhGuidance.length} records`);
console.log(`  Risk assessment templates: ${riskTemplates.length} records`);
console.log(`  FTS5 search index:       ${totalFts} entries`);
console.log(`  AT-vejledninger indexed: ${atVejledninger.length}`);
console.log(`  Kronesmiley indexed:     ${kronesmiley.length}`);
console.log(`  Database: data/database.db`);
