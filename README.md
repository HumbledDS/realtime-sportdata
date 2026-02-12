# ⚽ Du But au Buzz — Comment un Goal au Parc des Princes Arrive sur Ton Téléphone en 3 Secondes

> Architecture complète d'un système événementiel temps réel pour le football professionnel.
> Cas d'étude : PSG vs Marseille, Parc des Princes, Ligue 1.

---

## Table des Matières

- [Vue d'ensemble](#vue-densemble)
- [Étape 1 — Le but est marqué sur la pelouse](#étape-1--le-but-est-marqué-sur-la-pelouse)
- [Étape 2 — La captation de l'événement](#étape-2--la-captation-de-lévénement)
- [Étape 3 — La passerelle de bordure (Edge Gateway)](#étape-3--la-passerelle-de-bordure-edge-gateway)
- [Étape 4 — Le bus de messages (Apache Kafka)](#étape-4--le-bus-de-messages-apache-kafka)
- [Étape 5 — Le traitement en flux (Apache Flink)](#étape-5--le-traitement-en-flux-apache-flink)
- [Étape 6 — Le routage vers la notification](#étape-6--le-routage-vers-la-notification)
- [Étape 7 — L'envoi de la notification push](#étape-7--lenvoi-de-la-notification-push)
- [Étape 8 — Ton téléphone vibre](#étape-8--ton-téléphone-vibre)
- [Schéma JSON de l'événement GoalScored](#schéma-json-de-lévénement-goalscored)
- [Budget latence — Chronologie complète](#budget-latence--chronologie-complète)
- [Diagramme d'architecture complet](#diagramme-darchitecture-complet)
- [Garanties techniques](#garanties-techniques)
- [Que se passe-t-il si quelque chose plante ?](#que-se-passe-t-il-si-quelque-chose-plante-)
- [Au-delà de la notification — Qui d'autre consomme cet événement ?](#au-delà-de-la-notification--qui-dautre-consomme-cet-événement-)

---

## Vue d'ensemble

Démbélé marque à la 37ᵉ minute. Tu es dans les tribunes du Parc des Princes. Tu cries. Trois secondes plus tard, ton téléphone vibre dans ta poche : **"⚽ BUT ! Démbélé (PSG) — 2-0 [37']"**.

Ce document explique chaque étape technique entre le ballon qui franchit la ligne et cette notification sur ton écran de verrouillage.

```
   Ballon franchit la ligne
       │
       │  ~500ms
       ▼
   Hawk-Eye + Opérateur Opta détectent le but
       │
       │  ~50ms
       ▼
    Passerelle de bordure (Edge Gateway au stade)
       │
       │  ~20ms
       ▼
   Apache Kafka (bus de messages cloud)
       │
       │  ~50ms
       ▼
    Apache Flink (enrichissement + routage)
       │
       │  ~30ms
       ▼
   Service de notifications (segmentation + templating)
       │
       │  ~100ms
       ▼
   FCM / APNs (Firebase Cloud Messaging / Apple Push)
       │
       │  ~200ms–2s
       ▼
   Ton téléphone vibre
  
  ━━━━━━━━━━━━━━━━━━━━━━
  TOTAL : ~1 à 6 secondes
```

---

## Étape 1 — Le but est marqué sur la pelouse

Démbélé reprend un ballon en profondeur de Dembélé. Frappe du gauche. Le ballon entre dans le but.

À cet instant précis, **aucun système informatique ne sait encore qu'un but a été marqué**. L'événement existe uniquement dans le monde physique. Il faut maintenant le transformer en donnée numérique.

C'est le problème fondamental : **comment convertir un fait physique en un événement structuré exploitable par des machines, en moins d'une seconde ?**

La réponse : plusieurs systèmes captent le même fait simultanément, depuis des angles différents.

---

## Étape 2 — La captation de l'événement

Il n'y a pas un seul capteur magique qui détecte un but. En production, **plusieurs sources corroborent le même fait** :

### 2.1 La Goal-Line Technology (Hawk-Eye)

Quatorze caméras haute fréquence sont braquées en permanence sur chaque ligne de but. Le système Hawk-Eye reconstruit la position 3D du ballon 500 fois par seconde. Quand le ballon franchit entièrement la ligne, le système émet un **signal binaire** : `GOAL = true`.

Ce signal arrive sur la montre connectée de l'arbitre en **moins de 500 millisecondes**. C'est le même système qui valide les buts litigieux (le fameux "but fantôme").

```
14 caméras × 500 fps → reconstruction 3D du ballon → franchissement détecté
     │
     ▼
Signal binaire envoyé à la montre de l'arbitre
     │
     ▼
Transmission parallèle au système central du stade
```

**Limitation** : Hawk-Eye sait qu'un but est marqué, mais ne sait pas *qui* a marqué, ni *comment*. C'est un signal de confirmation, pas un événement riche.

### 2.2 L'opérateur de données en direct (Opta / Stats Perform)

Dans une cabine au stade, un **opérateur humain spécialisé** code chaque événement du match en temps réel sur un terminal dédié. Quand le but est marqué, il saisit :

- Le buteur (Démbélé)
- Le passeur (Mayulu)
- La minute (37')
- Le type de but (jeu ouvert)
- Le pied utilisé (gauche)
- Les coordonnées sur le terrain

Cet opérateur est entraîné pour coder un but en **3 à 8 secondes**. Son flux de données est riche et détaillé, mais plus lent que Hawk-Eye.

### 2.3 Le système de communication de l'arbitre

L'arbitre siffle et confirme le but verbalement via son oreillette. Le système de communication arbitrale (relié à la salle VAR) produit un signal de confirmation horodaté.

### 2.4 Les capteurs portés par les joueurs (GPS / IMU)

Chaque joueur porte un maillot équipé de capteurs GPS et d'accéléromètres (système EPTS). Ces capteurs tracent la position de chaque joueur 25 fois par seconde. Ils ne détectent pas le but directement, mais corroborent *où* était le buteur et *à quelle vitesse* il sprintait au moment de la frappe.

### Synthèse des sources

| Source | Ce qu'elle capte | Délai | Richesse |
|--------|-----------------|-------|----------|
| **Hawk-Eye** | Le ballon a franchi la ligne | < 500ms | Très faible (signal binaire) |
| **Opérateur Opta** | Qui, comment, d'où, passeur, minute | 3–8s | Très riche |
| **Arbitre (comms)** | Confirmation officielle | 1–3s | Faible |
| **GPS joueurs** | Position et vitesse du buteur | ~100ms | Moyenne (positionnelle) |

**Le but n'est pas un événement unique — c'est la fusion de signaux multiples.**

---

## Étape 3 — La passerelle de bordure (Edge Gateway)

Tous ces signaux convergent vers un serveur physiquement installé **dans le stade** : la **passerelle de bordure** (Edge Gateway). C'est un serveur rack situé dans les locaux techniques du Parc des Princes.

### Pourquoi un serveur au stade ?

Un stade le soir de PSG-OM est un **environnement réseau hostile**. 48 000 téléphones saturent les antennes 4G/5G. Le WiFi est congestionné. Le réseau cellulaire est imprévisible. On ne peut pas dépendre d'une connexion Internet stable entre le terrain et le cloud.

La passerelle résout ce problème :

```
┌──────────────────────────────────────────────────────────── ─┐
│              PASSERELLE DE BORDURE (Edge Gateway)            │
│              Parc des Princes — Salle technique              │
│                                                              │
│  1. RÉCEPTION                                                │
│     Reçoit les signaux de Hawk-Eye, Opta, arbitre, GPS       │
│                                                              │
│  2. CORRÉLATION                                              │
│     Le signal Hawk-Eye arrive à t=0.                         │
│     Le flux Opta arrive à t=4s avec les détails.             │
│     La passerelle fusionne les deux en un seul événement :   │
│     "But de Démbélé, passe de Dembélé, 37', pied gauche"     │
│                                                              │
│  3. DÉDUPLICATION                                            │
│     Si Hawk-Eye ET Opta rapportent le but, on ne crée        │
│     qu'un seul événement. La clé d'idempotence =             │
│     hash(match_id + minute + buteur) détecte les doublons.   │
│                                                              │
│  4. VALIDATION DU SCHÉMA                                     │
│     L'événement doit être conforme au schéma attendu.        │
│     Un événement malformé est rejeté ici, pas en aval.       │
│                                                              │
│  5. SÉQUENÇAGE                                               │
│     Attribution d'un numéro de séquence monotone croissant.  │
│     Événement 46 → 47 (le but) → 48 (le coup d'envoi         │
│     suivant). Cet ordre est garanti.                         │
│                                                              │
│  6. TAMPON LOCAL (Write-Ahead Log)                           │
│     L'événement est d'abord écrit sur le disque local        │
│     du serveur AVANT d'être envoyé au cloud.                 │
│     Si le réseau tombe, les événements s'accumulent          │
│     localement et sont rejoués à la reconnexion.             │
│                                                              │
│  7. ENVOI VERS KAFKA                                         │
│     L'événement est produit vers le cluster Kafka dans       │
│     le cloud via une connexion TLS/gRPC.                     │
│                                                              │
└──────────────────────────────────────────────────────── ─────┘
```

### Le tampon local — La bouée de sauvetage

Le **Write-Ahead Log** (WAL) est le mécanisme de survie de la passerelle. Chaque événement est persisté sur disque local **avant** d'être transmis. Si le réseau coupe pendant 30 secondes :

1. Les événements continuent d'arriver de Hawk-Eye et Opta
2. La passerelle les valide et les stocke localement
3. Le réseau revient
4. La passerelle rejoue tous les événements bufférisés dans l'ordre
5. Kafka reçoit le flux complet, sans perte

Capacité du tampon : environ 10 000 événements ou 1 Go. Largement suffisant pour couvrir une coupure de plusieurs minutes.

**Temps passé dans cette étape : ~50ms** (validation + sérialisation + production Kafka)

---

## Étape 4 — Le bus de messages (Apache Kafka)

L'événement quitte le stade et arrive dans **Apache Kafka**, un système de messagerie distribué hébergé dans le cloud (Confluent Cloud ou auto-géré sur Kubernetes).

### Pourquoi Kafka ?

Kafka est le standard de l'industrie pour le streaming événementiel à grande échelle. Voici pourquoi il a été choisi plutôt que Google Pub/Sub ou AWS Kinesis :

| Critère | Kafka | Alternatives |
|---------|-------|-------------|
| **Ordre garanti** | Oui, par partition | Pub/Sub : limité. Kinesis : par shard. |
| **Rejeu** | Depuis n'importe quel offset | Pub/Sub : par timestamp seulement |
| **Exactement une fois** | Natif (producteur idempotent) | Pas natif chez les concurrents |
| **Registre de schémas** | Confluent Schema Registry | Pas d'équivalent natif |
| **Dépendance vendeur** | Aucune (open source) | Pub/Sub = GCP, Kinesis = AWS |

### Comment Kafka organise les données

Kafka stocke les événements dans des **topics** (files de messages thématiques). Chaque topic est divisé en **partitions**. Les événements d'un même match atterrissent toujours dans la même partition, car la clé de partitionnement est le `match_id`.

```
Topic : match-events-raw
│
├── Partition 0 : PSG vs OM (tous les événements de ce match)
├── Partition 1 : Lyon vs Monaco
├── Partition 2 : Lille vs Lens
└── ...

Chaque partition est un journal ordonné et immuable :
┌──────┬──────┬──────┬──────┬──────┬───── ─┬──────┐
│ ev.42│ ev.43│ ev.44│ ev.45│ ev.46│ ev.47 │ ev.48│
│ coup │corner│ tir  │ arrêt│corner│  BUT  │coup  │
│envoi │      │cadré │      │      │Démbélé│envoi │
└──────┴──────┴──────┴──────┴──────┴────── ┴──────┘
                                      ▲
                                Le but est ici,
                              à sa place dans l'ordre
```

**Garantie fondamentale** : au sein d'une partition, l'ordre est strictement garanti. Le but de Démbélé (événement 47) arrivera **toujours** après le corner (46) et **toujours** avant le coup d'envoi (48). C'est essentiel — on ne peut pas notifier un but annulé par la VAR avant le but lui-même.

### Configuration du producteur

```properties
acks=all                                    # Le message est confirmé seulement quand
                                            # TOUS les réplicas l'ont écrit
enable.idempotence=true                     # Pas de duplication même en cas de retry
max.in.flight.requests.per.connection=5     # Parallélisme contrôlé
compression.type=lz4                        # Compression rapide
linger.ms=5                                 # Attendre max 5ms pour battre
                                            # (compromis latence/débit)
```

### Réplication et durabilité

Chaque événement est répliqué sur **3 brokers Kafka** différents. Si un serveur tombe, les deux autres ont toujours une copie complète. Le paramètre `min.insync.replicas=2` garantit qu'au moins 2 réplicas confirment chaque écriture.

```
Événement "But de Démbélé"
     │
     ├──→ Broker 1 (leader)   ✅ écrit
     ├──→ Broker 2 (follower) ✅ écrit
     └──→ Broker 3 (follower) ✅ écrit
     
     Confirmation renvoyée au producteur : "ack"
```

**Temps passé dans cette étape : ~20ms** (production + réplication sur 3 brokers)

---

## Étape 5 — Le traitement en flux (Apache Flink)

L'événement est maintenant dans Kafka, mais il est encore **brut**. Il contient un `player_id` mais pas le nom complet du joueur. Il contient un `team_id` mais pas le logo de l'équipe. Il manque le xG (expected goals) de la frappe. Il faut l'**enrichir**.

C'est le rôle d'**Apache Flink**, un moteur de traitement de flux en temps réel.

### Pourquoi Flink et pas Spark ?

Apache Spark Structured Streaming fonctionne en **micro-batchs** : il accumule les événements pendant 100ms minimum, puis les traite d'un bloc. Ce plancher de 100ms est inacceptable quand chaque milliseconde compte.

Flink traite chaque événement **individuellement, dès qu'il arrive**. C'est du vrai streaming, pas du batch rapide.

### Ce que fait le job Flink d'enrichissement

```
Événement brut (Kafka)
     │
     ▼
┌────────────────────────────────────────────────┐
│           JOB FLINK : ENRICHISSEMENT           │
│                                                │
│  1. Désérialiser l'événement (Avro/Protobuf)   │
│                                                │
│  2. Joindre avec la base joueurs (Redis)       │
│     player_id: "PLR-Démbélé-10"                │
│     → name: "Ousmane Démbélé"                  │
│     → jersey: 10, position: "AV"               │
│     → photo_url: "https://..."                 │
│     (Appel asynchrone, non-bloquant)           │
│                                                │
│  3. Mettre à jour l'état du match (Flink State)│
│     Score avant : 1-0                          │
│     Score après : 2-0                          │
│     Ce score est maintenu dans la mémoire      │
│     de Flink, partitionné par match_id.        │
│                                                │
│  4. Calculer les champs dérivés                │
│     xG de la frappe : 0.76                     │
│     Distance de tir : 14.2m                    │
│     Vitesse du ballon : 102 km/h               │
│                                                │
│  5. Déduplication idempotente                  │
│     L'idempotency_key est stocké dans l'état.  │
│     Si un doublon arrive (même hash), il est   │
│     silencieusement ignoré.                    │
│                                                │
│  6. Produire l'événement enrichi               │
│     → topic Kafka : match-events-enriched      │
└────────────────────────────────────────────────┘
```

### L'état Flink — La mémoire vivante du match

Flink maintient un **état local** par match (stocké dans RocksDB sur disque). Cet état contient :

- Le score actuel
- La liste des buteurs
- Les cartons distribués
- Les remplacements effectués
- La minute de jeu

Quand un but arrive, Flink incrémente le score dans son état et attache `score_after: { home: 2, away: 1 }` à l'événement. Cet état est **sauvegardé toutes les 30 secondes** (checkpoint) sur un stockage distant (S3). Si le job plante, il redémarre exactement où il s'était arrêté.

**Temps passé dans cette étape : ~50ms** (enrichissement + requête Redis async + production)

---

## Étape 6 — Le routage vers la notification

L'événement enrichi arrive dans le topic `match-events-enriched`. Un **deuxième job Flink** le consomme : le **routeur de notifications**.

Son rôle : décider **qui doit être notifié** et **avec quel contenu**.

```
Événement enrichi : "But de Démbélé, PSG 2-0 OM, 37'"
     │
     ▼
┌────────────────────────────────────────────────────────┐
│         JOB FLINK : ROUTEUR DE NOTIFICATIONS           │
│                                                        │
│  1. SEGMENTATION DES UTILISATEURS                      │
│     Qui veut savoir ?                                  │
│                                                        │
│     Segment A : Fans du PSG (abonnés à l'équipe)       │
│     Segment B : Suiveurs de Ligue 1 (abonnés au        │
│                 championnat)                           │
│     Segment C : Joueurs de fantasy qui ont Démbélé     │
│                 dans leur équipe                       │
│     Segment D : Parieurs avec un pari en cours sur     │
│                 ce match                               │
│                                                        │
│  2. PRIORITÉ                                           │
│     GOAL_SCORED → priorité HAUTE                       │
│     (notifications immédiates, vibration, son)         │
│                                                        │
│     YELLOW_CARD → priorité NORMALE                     │
│     (notification silencieuse)                         │
│                                                        │
│     SUBSTITUTION → priorité BASSE                      │
│     (notification regroupée, pas d'alerte)             │
│                                                        │
│  3. TEMPLATING DU CONTENU                              │
│                                                        │
│     FR : "⚽ BUT ! Démbélé (PSG) — 2-0 [37']"         │
│     EN : "⚽ GOAL! Démbélé (PSG) — 2-0 [37']"         │
│     AR : "⚽ !هدف مبابي (باريس) — 2-0 [37']"         │
│                                                        │
│  4. CONTRÔLE DE DÉBIT                                  │
│     Max 1 notification par événement par utilisateur   │
│     Max 10 notifications par match par utilisateur     │
│     (éviter le spam pendant un match à 6 buts)         │
│                                                        │
│  5. PRODUCTION                                         │
│     → topic Kafka : match-events-notifications         │
│     Clé de partition : hash du segment utilisateur     │
│     (répartition uniforme sur les workers en aval)     │
└────────────────────────────────────────────────────────┘
```

**Temps passé dans cette étape : ~30ms**

---

## Étape 7 — L'envoi de la notification push

L'événement atterrit dans le topic `match-events-notifications`. Des **workers de dispatch** le consomment et transforment la notification en appels concrets vers les plateformes de push.

### Le service de dispatch

```
match-events-notifications (Kafka)
     │
     ▼
┌────────────────────────────────────────────────────────┐
│            SERVICE DE DISPATCH PUSH                    │
│            (cluster de 200 pods Kubernetes)            │
│                                                        │
│  1. RÉSOLUTION DES TOKENS                              │
│     Le segment "Fans PSG" = 2.3 millions d'appareils   │
│     Chaque appareil a un token de notification :       │
│     - Token FCM (Android / Web)                        │
│     - Token APNs (iPhone / iPad)                       │
│     Ces tokens sont stockés dans une base dédiée       │
│     (Postgres partitionné ou DynamoDB)                 │
│                                                        │
│  2. CONSTITUTION DES BATCHS                            │
│                                                        │
│     Firebase Cloud Messaging (FCM) :                   │
│     Accepte jusqu'à 500 tokens par requête HTTP        │
│     2.3M appareils ÷ 500 = 4 600 requêtes              │
│                                                        │
│     Apple Push Notification service (APNs) :           │
│     Connexion HTTP/2 multiplexée, envoi en parallèle   │
│                                                        │
│  3. ENVOI PARALLÉLISÉ                                  │
│     200 pods × 50 requêtes parallèles = 10 000         │
│     requêtes simultanées                               │
│     4 600 requêtes FCM ÷ 10 000 workers = < 1 seconde  │
│                                                        │
│  4. GESTION DES ERREURS                                │
│     Token invalide → supprimer de la base              │
│     Erreur réseau → retry avec backoff exponentiel     │
│     Échec après 3 tentatives → dead letter queue       │
│     Taux d'échec > 5% → alerte ops                     │
│                                                        │
│  5. CONFIRMATION                                       │
│     Offset Kafka commité seulement APRÈS confirmation  │
│     de livraison par FCM/APNs.                         │
│     Garantie : au-moins-une-fois (at-least-once)       │
└────────────────────────────────────────────────────────┘
```

### Pourquoi "au-moins-une-fois" et pas "exactement-une-fois" ?

Garantir "exactement-une-fois" pour les notifications push est extrêmement coûteux (il faudrait un protocole de commit en deux phases avec FCM/APNs, ce qui est impossible car on ne contrôle pas ces services). Le compromis est pragmatique :

- Recevoir **deux fois** la notification "But de Démbélé" est agaçant mais sans conséquence
- **Ne pas recevoir** la notification est inacceptable

Donc on choisit at-least-once : en cas de doute (crash entre l'envoi et la confirmation), on renvoie. Le téléphone déduplique généralement les notifications identiques.

**Temps passé dans cette étape : ~100ms** (batching + envoi HTTP)

---

## Étape 8 — Ton téléphone vibre

### Côté serveur (FCM / APNs)

Firebase (Google) et APNs (Apple) reçoivent les requêtes de notification et les **routent vers l'appareil cible** via leur infrastructure réseau propriétaire.

```
Dispatch Service → requête HTTP/2 → serveurs FCM (Google)
                                          │
                                          ▼
                                   Réseau interne Google
                                          │
                                          ▼
                                   Connexion persistante
                                   avec ton téléphone Android
                                   (ou via GMS sur certains iPhone)
                                          │
                                          ▼
                                    Notification affichée
```

Pour les iPhones, le chemin passe par APNs :

```
Dispatch Service → requête HTTP/2 → serveurs APNs (Apple)
                                          │
                                          ▼
                                   Réseau interne Apple
                                          │
                                          ▼
                                   Connexion persistante
                                   avec ton iPhone
                                          │
                                          ▼
                                    Notification affichée
```

### Côté téléphone

1. Le système d'exploitation (Android/iOS) reçoit la notification via sa connexion persistante avec FCM/APNs
2. L'application sportive (Ligue 1, OneFootball, L'Équipe...) est réveillée en arrière-plan
3. Le contenu est formaté selon le template reçu :
   ```
   ┌─────────────────────────────────────────┐
   │  Ligue 1                    il y a 3s   │
   │                                         │
   │ BUT ! Démbélé (PSG) — 2-0 [37']         │
   │ Passe décisive : Mayulu                 │
   └─────────────────────────────────────────┘
   ```
4. Vibration, son, affichage sur l'écran de verrouillage
5. Si tu ouvres l'app, une **connexion WebSocket** prend le relais pour le flux en temps réel

**Temps passé dans cette étape : 200ms à 2 secondes** (variable selon le réseau mobile et la latence FCM/APNs)

---

## Schéma JSON de l'événement GoalScored

Voici l'événement tel qu'il circule dans Kafka après enrichissement :

```json
{
  "event_id": "0190d4a2-7b3c-7d4e-8f5a-1b2c3d4e5f6a",
  "event_type": "GOAL_SCORED",
  "event_version": 3,
  "match_id": "LFP-2425-PSG-OM-0315",

  "competition": {
    "id": "ligue1-2024-25",
    "name": "Ligue 1 2024-25",
    "matchday": 28
  },

  "occurred_at": "2025-03-15T20:37:12.450Z",
  "received_at": "2025-03-15T20:37:12.823Z",
  "published_at": "2025-03-15T20:37:13.001Z",
  "sequence_number": 47,

  "source": {
    "system": "OPTA",
    "operator_id": "opta-op-FR-042",
    "confidence": 1.0,
    "stadium_gateway_id": "gw-parc-des-princes-01"
  },

  "data": {
    "scorer": {
      "player_id": "PLR-Démbélé-10",
      "name": "Kylian Démbélé",
      "jersey_number": 7,
      "position": "AV"
    },
    "assist": {
      "player_id": "PLR-MAYULU-24",
      "name": "Senny MAYULU",
      "assist_type": "PASSE_EN_PROFONDEUR"
    },
    "team": {
      "team_id": "TEAM-PSG",
      "name": "Paris Saint-Germain",
      "side": "HOME"
    },
    "match_minute": 37,
    "match_period": "FIRST_HALF",
    "score_after": { "home": 2, "away": 1 },
    "goal_type": "OPEN_PLAY",
    "body_part": "LEFT_FOOT",
    "coordinates": { "x": 92.3, "y": 45.1 },
    "expected_goals": 0.76,
    "var_reviewed": false,
    "var_decision": null
  },

  "metadata": {
    "idempotency_key": "sha256:a3f2b8c1d4e5f6...",
    "correlation_id": "corr-0190d4a2-goal-37",
    "causation_id": null,
    "ttl_seconds": 300
  }
}
```

### Les trois horodatages expliqués

| Champ | Signification | Valeur dans notre exemple |
|-------|--------------|--------------------------|
| `occurred_at` | Quand le ballon a franchi la ligne | 20:37:12.450 |
| `received_at` | Quand la passerelle du stade a reçu le signal | 20:37:12.823 (+373ms) |
| `published_at` | Quand Kafka a persisté l'événement | 20:37:13.001 (+551ms) |

Ces trois timestamps permettent de **mesurer la latence à chaque étape** en production. Si `published_at - occurred_at` dépasse 2 secondes, une alerte se déclenche.

---

## Budget latence — Chronologie complète

```
20:37:12.450   Le ballon franchit la ligne de but
                │
                │  373ms — Hawk-Eye détecte + opérateur Opta code
                │          + passerelle de bordure traite
                ▼
20:37:12.823    Événement reçu par la passerelle du stade
                │
                │  178ms — Validation, sérialisation, production Kafka
                │          + réplication sur 3 brokers
                ▼
20:37:13.001   Événement persisté dans Kafka
                │
                │  50ms — Job Flink d'enrichissement
                │         (jointure Redis, calcul xG, mise à jour état)
                ▼
20:37:13.051    Événement enrichi produit dans Kafka
                │
                │  30ms — Job Flink de routage notifications
                │         (segmentation, templating, rate limiting)
                ▼
20:37:13.081   Événement notification prêt dans Kafka
                │
                │  100ms — Dispatch service : résolution tokens,
                │          batching, envoi HTTP/2 vers FCM/APNs
                ▼
20:37:13.181   Requête envoyée à FCM / APNs
                │
                │  200ms–2s — Réseau Google/Apple → réseau mobile
                │             → appareil → affichage
                ▼
20:37:13.4–15.2  TON TÉLÉPHONE VIBRE

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Latence totale : 1 à 3 secondes (cas nominal)
                 jusqu'à 6 secondes (réseau mobile dégradé)
```

---

## Diagramme d'architecture complet

```
┌──────────────────── PARC DES PRINCES ────────────────────┐
│                                                          │
│  Hawk-Eye    Opta      Arbitre     GPS Joueurs           │
│  (14 cam.)  (opérateur) (comms)    (EPTS)                │
│      │          │          │           │                 │
│      └──────────┴──────────┴───────────┘                 │
│                      │                                   │
│              ┌───────▼────────┐                          │
│              │  PASSERELLE    │ ← Disque local (WAL)     │
│              │  DE BORDURE    │                          │
│              │  (Edge Gateway)│                          │
│              └───────┬────────┘                          │
└──────────────────────┼───────────────────────────────────┘
                       │ TLS / gRPC
                       ▼
            ┌──────────────────────┐
            │    APACHE KAFKA      │
            │    (3 brokers,       │
            │     RF=3, ISR=2)     │
            │                      │
            │  ┌────────────────┐  │
            │  │ match-events-  │  │  ← Événements bruts
            │  │ raw            │  │
            │  └───────┬────────┘  │
            └──────────┼───────────┘
                       │
            ┌──────────▼───────────┐
            │  APACHE FLINK        │
            │  Job: Enrichissement │
            │  (+ Redis async)     │
            └──────────┬───────────┘
                       │
            ┌──────────▼───────────┐
            │    APACHE KAFKA      │
            │  ┌────────────────┐  │
            │  │ match-events-  │  │  ← Événements enrichis
            │  │ enriched       │  │
            │  └──┬───┬───┬──┬─┘  │
            └─────┼───┼───┼──┼────┘
                  │   │   │  │
       ┌──────────┘   │   │  └──────────────┐
       │              │   │                 │
       ▼              ▼   ▼                 ▼
 ┌──────────┐  ┌─────────────┐  ┌────────────────┐
 │  Flink   │  │   Flink     │  │   Kafka →      │
 │  Routeur │  │   Trigger   │  │   ClickHouse   │
 │  Notifs  │  │   Cotes     │  │   (Analytics)  │
 └────┬─────┘  └──────┬──────┘  └────────────────┘
      │               │
      ▼               ▼
 ┌──────────┐  ┌─────────────┐
 │ Dispatch │  │  Moteur de  │
 │ Push     │  │  cotes      │
 │ (200pods)│  │  (betting)  │
 └────┬─────┘  └─────────────┘
      │
      ├──→ FCM (Android) ──→ Push Notification
      └──→ APNs (iPhone) ──→ Push Notification
```

---

## Garanties techniques

### Livraison des messages

| Segment du pipeline | Garantie | Comment |
|---------------------|----------|---------|
| Stade → Kafka | **Exactement une fois** | Producteur idempotent + WAL local |
| Kafka → Flink | **Exactement une fois** | Checkpointing Flink + transactions Kafka |
| Flink → Kafka | **Exactement une fois** | Commit en deux phases (2PC sink) |
| Kafka → Dispatch push | **Au moins une fois** | Commit d'offset après confirmation FCM/APNs |
| Kafka → WebSocket | **Au moins une fois** | Dédup côté client via `event_id` |
| Kafka → Betting | **Exactement une fois** | Consommateur transactionnel + idempotence |

### Ordre des événements

Tous les événements d'un même match partagent la même partition Kafka (clé = `match_id`). L'ordre est **strictement garanti** au sein d'une partition.

Concrètement : tu ne recevras **jamais** la notification "But annulé par la VAR" avant "But de Démbélé". L'architecture l'interdit structurellement.

### Idempotence

Chaque événement porte une `idempotency_key` = hash(match_id + event_type + minute + player_id). Si le même but est signalé par deux sources différentes, le hash sera identique et l'événement doublon sera supprimé silencieusement à chaque étape : passerelle, Flink, dispatch.

---

## Que se passe-t-il si quelque chose plante ?

| Panne | Impact | Récupération |
|-------|--------|-------------|
| **Réseau du stade tombe** | Les événements s'accumulent dans le WAL de la passerelle | Rejeu automatique à la reconnexion. Aucune perte. |
| **Un broker Kafka meurt** | Aucun impact visible. Les 2 autres réplicas prennent le relais. | Le contrôleur Kafka rééquilibre les partitions. |
| **Le job Flink plante** | Traitement interrompu pendant ~10 secondes. | Redémarrage depuis le dernier checkpoint (S3). Reprise exacte. |
| **La gateway WebSocket plante** | Les clients connectés sont déconnectés. | Reconnexion automatique + rattrapage via API REST. |
| **FCM/APNs est lent** | Les notifications arrivent avec 2-5s de retard supplémentaire. | Retry avec backoff exponentiel. Dead letter après 3 échecs. |
| **Redis tombe** | L'enrichissement est dégradé (noms manquants). | Bascule vers les réplicas Redis. Fallback Postgres. |
| **Un message est corrompu** | Il ne passe pas la validation de schéma. | Routé vers la Dead Letter Queue (DLQ). Alerte ops. |

La philosophie : **chaque composant est conçu pour tomber sans perdre de données**. Le WAL protège le stade, Kafka protège le transport, les checkpoints Flink protègent le traitement, et les retries protègent la livraison.

---

## Au-delà de la notification — Qui d'autre consomme cet événement ?

Le but de Démbélé ne produit pas seulement une notification sur ton téléphone. Le même événement, consommé depuis le même topic Kafka, déclenche simultanément :

```
Un seul événement "GOAL_SCORED"
         │
         ├──→  5M de notifications push (fans, fantasy, parieurs)
         │
         ├──→  Mise à jour des graphiques TV (score incrusté à l'écran)
         │         Canal+, DAZN, beIN Sports consomment un flux dédié
         │
         ├──→  Suspension des marchés de paris en 50ms
         │         Recalcul des cotes (résultat final, buteur, score exact)
         │         Réouverture des marchés avec nouvelles cotes
         │
         ├──→  Dashboard analytique mis à jour
         │         xG cumulé du match, carte des tirs, stats possession
         │
         ├──→   Écrans du stade mis à jour (score, nom du buteur)
         │
         ├──→  Scores fantasy actualisés (Sorare, Mon Petit Gazon)
         │
         ├──→  Bots réseaux sociaux publient automatiquement
         │         "@PSG_inside: ⚽ 37' BUUUUT DE DEMBELE ! 2-0"
         │
         └──→  Modèle ML de probabilité de victoire recalculé
                  PSG win probability : 62% → 78%
```

**Un seul événement. Un seul topic Kafka. Des dizaines de consommateurs indépendants.** C'est la puissance d'une architecture événementielle découplée : chaque consommateur avance à son rythme, sans impacter les autres.

---

## Conclusion

Du cuir qui touche le filet au buzz dans ta poche, il s'écoule entre **1 et 6 secondes**. Ce délai cache une chaîne de 8 systèmes distincts, chacun optimisé pour son rôle :

1. **Captation** — Hawk-Eye pour la vitesse, Opta pour la richesse
2. **Edge** — La passerelle absorbe l'hostilité du réseau du stade
3. **Transport** — Kafka garantit l'ordre, la durabilité, et le découplage
4. **Traitement** — Flink enrichit en temps réel sans micro-batch
5. **Routage** — Flink segmente et personnalise
6. **Dispatch** — 200 pods envoient 5 millions de notifications en parallèle
7. **Livraison** — FCM/APNs acheminent jusqu'au dernier kilomètre
8. **Affichage** — Le système mobile réveille l'app et affiche la notification

Chaque maillon est conçu pour **tomber sans perdre de données** et **redémarrer sans intervention humaine**. C'est ça, une architecture de production.

---

*Architecture conçue pour un système événementiel temps réel en football professionnel. Les patterns décrits ici sont utilisés en production par Stats Perform, Sportradar, Genius Sports, et les principales plateformes de paris sportifs.*