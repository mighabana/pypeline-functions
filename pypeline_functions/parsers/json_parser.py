from datetime import UTC, datetime


class GoogleTakeout:
    """Namespace for all the JSON parser functions for the Google Takeout data.

    Methods
    -------
    chrome_history_parser(dct:dict) -> dict
        Parse and format a single entry of the Google Chrome History data.
    activity_parser(dct:dict) -> dict
        Parse and format a single entry of the Google Activity data.
    _candidate_location_parser(dct:dict) -> dict
        Parse and format a single entry of the candidate locations from the Semantic Location History data.
    location_parser(dct:dict) -> dict
        Parse and format a single entry of the Semantic Location History data.
    """

    def chrome_history_parser(self, dct:dict) -> dict:
        """
        Parse and format a single entry of the Google Chrome History data.

        Parameters
        ----------
        dct : dict
            A dictionary representing a single entry of the Google Chrome History data.
        """
        dct["title"] = dct.get("title", "")
        dct["page_transition"] = dct.get("page_transition", "")
        if isinstance(dct.get("ptoken", {}), dict) and len(dct.get("ptoken", {}) == 0):
            dct["ptoken"] = None
        else:
            # might be an unnecessary statement but leaving it in to be safe
            dct["ptoken"] = dct.get("ptoken", None)
        # TODO: add HTTP sanitation by converting to HTTPS
        dct["url"] = dct.get("url", "")
        dct["time_usec"] = datetime.fromtimestamp(dct.get("time_usec", 0) / 10**6, UTC)


    def activity_parser(self, dct:dict) -> dict:
        """
        Parse and format a single entry of the Google Activity data.

        Parameters
        ----------
        dct : dict
            A dictionary representing a single entry of the Google Activity data.
        """
        datetime_format = "%Y-%m-%dT%H:%M:%S.%fZ"
        dct["header"] = dct.get("header")
        dct["title"] = dct.get("title")
        for datetime_format in ["%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ"]:
            try:
                dct["time"] = datetime.strptime(dct.get("title", "1970-01-01:00:00:00Z"), datetime_format)  # noqa: DTZ007
                break
            except ValueError:
                pass
        dct["description"] = dct.get("description")
        dct["titleUrl"] = dct.get("titleUrl")
        subtitles = dct.get("subtitles", [])
        _subtitles = []
        for subtitle in subtitles:
            _subtitles.append({
                "name": subtitle.get("name", ""),
                "url": subtitle.get("url", None)
            })
        dct["subtitles"] = _subtitles
        details = dct.get("details", [])
        _details = []
        for detail in details:
            _details.append({
                "name": detail.get("name", "")
            })
        dct["details"] = _details
        dct["products"] = dct.get("products")
        dct["activityControls"] = dct.get("activityControls")
        return dct

    def _candidate_location_parser(self, dct:dict) -> dict:
        """
        Parse and format a single entry of the candidate locations from the Semantic Location History data.

        Parameters
        ----------
        dct : dict
            A dictionary representing a single entry of the candidate locations from the Semantic Location History data.
        """
        output = {}

        output["lat"] = dct["centerLatE7"]
        output["lng"] = dct["centerLngE7"]
        output["place_id"] = dct["placeId"]
        output["semantic_type"] = dct.get("semanticType", None)
        output["address"] = dct.get("address", None)
        output["name"] = dct.get("name", None)
        output["location_confidence"] = dct.get("locationConfidence", None)

        return output

    def location_parser(self, dct:dict) -> dict:
        """
        Parse and format a single entry of the Semantic Location History data.

        Parameters
        ----------
        dct : dict
            A dictionary representing a single entry of the Semantic Location History data.
        """
        output = {}
        datetime_format = "%Y-%m-%dT%H:%M:%SZ"

        place_visit = dct["placeVisit"]
        location = place_visit["location"]
        duration = place_visit["duration"]
        output["lat"] = location.latitudeE7
        output["lng"] = location.longitudeE7
        output["place_id"] = location.placeId
        output["location_confidence"] = location.locationConfidence
        output["address"] = location.get("address", None)
        output["name"] = location.get("name", None)
        output["calibrated_probability"] = location.get("calibratedProbability", None)
        output["device_tag"] = location.get("sourceInfo", {"deviceTag": None}).deviceTag
        if duration.startTimestamp is None:
            output["start_time"] = None
        else:
            output["start_time"] = datetime.strptime(duration.startTimestamp, datetime_format) # noqa: DTZ007
        if duration.endTimestamp is None:
            output["end_time"] = None
        else:
            output["end_time"] = datetime.strptime(duration.endTimestamp, datetime_format) # noqa: DTZ007
        output["center_lat"] = dct.get("centerLatE7", None)
        output["center_lng"] = dct.get("centerLngE7", None)
        output["place_confidence"] = dct.get("placeConfidence", None)
        output["place_visit_type"] = dct.get("placeVisitType", None)
        output["visit_confidence"] = dct.get("visitConfidence", None)
        output["edit_confirmation_status"] = dct.get("editConfirmationStatus", None)
        output["place_visit_importance"] = dct.get("placeVisitImportance", None)

        parsed_locations = []
        candidate_locations = dct.get("otherCandidateLocations", [])
        for candidate_location in candidate_locations:
            loc_parsed = self._candidate_location_parser(candidate_location)
            parsed_locations.append(loc_parsed)
        output["candidate_locations"] = parsed_locations

        return output

class Spotify:
    """
    Namespace for all the JSON parser functions for the Spotify data.

    Methods
    -------
    follow_data_parser(dct:dict) -> dict
        Parse and format the follow data.
    identifier_parser(dct:dict) -> dict
        Parse and format the identifier data.
    marquee_parser(dct:dict) -> dict
        Parse and format a single entry of the Marquee data.
    search_query_parser(dct:dict) -> dict
        Parse and format a single entry of the Search Query data.
    user_data_parser(dct:dict) -> dict
        Parse and format the User data.
    _track_parser(dct:dict) -> dict
        Parse and format a single entry of track data.
    _album_parser(dct:dict) -> dict
        Parse and format a single entry of album data.
    _artist_parser(dct:dict) -> dict
        Parse and format a single entry of artist data.
    library_parser(dct:dict) -> dict
        Parse and format a single entry of Music Library data.
    streaming_history_parser(dct:dict) -> dict
        Parse and format a single entry of Streaming History data.
    """

    def follow_data_parser(self, dct:dict) -> dict:
        """
        Parse and format the follow data.

        Parameters
        ----------
        dct : dict
            A dictionary representing the follow data.
        """
        output = {}

        output["follower_count"] = int(dct["followerCount"])
        output["following_users_count"] = int(dct["followingUsersCount"])
        output["dismissing_users_count"] = int(dct["dismissingUsersCount"])

        return output

    def identifier_parser(self, dct:dict) -> dict:
        """
        Parse and format the identifier data.

        Parameters
        ----------
        dct : dict
            A dictionary representing the identifier data.
        """
        output = {}

        output["identifier_type"] = dct["identifierType"]
        output["identifier_value"] = dct["identifierValue"]

        return output

    def marquee_parser(self, dct:dict) -> dict:
        """
        Parse and format a single entry of the Marquee data.

        Parameters
        ----------
        dct : dict
            A dictionary representing a single entry of the Marquee data.
        """
        output = {}

        output["artist_name"] = dct["artistName"]
        output["segment"] = dct["segment"]

        return output

    def search_query_parser(self, dct:dict) -> dict:
        """
        Parse and format the Search Query data.

        Parameters
        ----------
        dct : dict
            A dictionary representing the Search Query data.
        """
        output = {}

        output["platform"] = dct["platform"]
        if dct["searchTime"] is not None:
            output["search_time"] = datetime.strptime(dct["searchTime"][:19], "%Y-%m-%dT%H:%M:%S")  # noqa: DTZ007
        else:
            output["search_time"] = None
        output["search_query"] = dct["searchQuery"]
        if (len(dct["searchInteractionURIs"]) == 0):
            output["search_interaction_URIs"] = None
        else:
            output["search_interaction_URIs"] = dct.get("searchInteractionURIs", None)

        return output

    def user_data_parser(self, dct:dict) -> dict:
        """
        Parse and format the user data.

        Parameters
        ----------
        dct : dict
            A dictionary representing the user data.
        """
        output = {}

        output["username"] = dct.get("username", None)
        output["email"] = dct["email"]
        output["country"] = dct["country"]
        output["created_from_facebook"] = dct["createdFromFacebook"]
        output["facebook_UID"] = dct.get("facebookUid", None)
        output["birthdate"] = datetime.strptime(dct["birthdate"][:10], "%Y-%m-%d") # noqa: DTZ007
        output["gender"] = dct["gender"]
        output["postal_code"] = dct.get("postalCode", None)
        output["mobile_number"] = dct.get("mobileNumber", None)
        output["mobile_operator"] = dct.get("mobileOperator", None)
        output["mobile_brand"] = dct.get("mobileBrand", None)
        output["creation_time"] = dct.get("creationTime", None)

        return output

    def _track_parser(self, dct:dict) -> dict:
        """
        Parse and format a single entry of track data.

        Parameters
        ----------
        dct : dict
            A dictionary representing a single entry of track data.
        """
        output = {}

        output["artist"] = dct.get("artist", "")
        output["album"] = dct.get("album", "")
        output["track"] = dct.get("track", "")
        output["uri"] = dct.get("uri", "")

        return output

    def _album_parser(self, dct:dict) -> dict:
        """
        Parse and format a single entry of album data.

        Parameters
        ----------
        dct : dict
            A dictionary representing a single entry of album data.
        """
        output = {}

        output["artist"] = dct.get("artist", "")
        output["album"] = dct.get("album", "")
        output["uri"] = dct.get("uri", "")

        return output

    def _artist_parser(self, dct:dict) -> dict:
        """
        Parse and format a single entry of artist data.

        Parameters
        ----------
        dct : dict
            A dictionary representing a single entry of artist data.
        """
        output = {}

        output["name"] = dct.get("name", "")
        output["uri"] = dct.get("uri", "")

        return output

    def library_parser(self, dct:dict) -> dict:
        """
        Parse and format a single entry of Music Library data.

        Parameters
        ----------
        dct : dict
            A dictionary representing a signle entry of Music Library data.
        """
        output = {}

        output["tracks"] = [self._track_parser(datum) for datum in dct.get("tracks", [])]
        output["albums"] = [self._album_parser(datum) for datum in dct.get("albums", [])]
        output["shows"] = dct.get("shows", None)
        output["episodes"] = dct.get("episodes", None)
        if (len(dct["bannedTracks"]) == 0) | (dct["bannedTracks"] is None):
            output["banned_tracks"] = None
        else:
            output["banned_tracks"] = [self._track_parser(datum) for datum in dct.get("bannedTracks", [])]
        output["artists"] = [self._artist_parser(datum) for datum in dct.get("artists", [])]
        if (len(dct["bannedArtists"]) == 0) | (dct["bannedArtists"] is None):
            output["banned_artists"] = None
        else:
            output["banned_artists"] = [self._artist_parser(datum) for datum in dct.get("bannedArtists", [])]
        output["other"] = dct.get("other", None)

        return output

    def streaming_history_parser(self, dct:dict) -> dict:
        """
        Parse and format a single entry of Streaming History data.

        Parameters
        ----------
        dct : dict
            A dictionary representing a single entry of Streaming History data.
        """
        output = {}

        output["ts"] = datetime.strptime(dct["ts"], "%Y-%m-%dT%H:%M:%SZ")  # noqa: DTZ007
        output["username"] = dct.get("username", "")
        output["platform"] = dct.get("platform", "")
        output["ms_played"] = dct.get("ms_played", None)
        output["conn_country"] = dct.get("conn_country", "")
        output["ip_addr_decrypted"] = dct.get("ip_addr_decrypted", "")
        output["user_agent_decrypted"] = dct.get("user_agent_decrypted", "")
        output["master_metadata_track_name"] = dct.get("master_metadata_track_name", None)
        output["master_metadata_album_artist_name"] = dct.get("master_metadata_album_artist_name", None)
        output["master_metadata_album_album_name"] = dct.get("master_metadata_album_album_name", None)
        output["spotify_track_uri"] = dct.get("spotify_track_uri", None)
        output["episode_name"] = dct.get("episode_name", None)
        output["episode_show_name"] = dct.get("episode_show_name", None)
        output["spotify_episode_uri"] = dct.get("spotify_episode_uri", None)
        output["reason_start"] = dct.get("reason_start", None)
        output["reason_end"] = dct.get("reason_end", None)
        output["shuffle"] = dct.get("shuffle", None)
        output["skipped"] = dct.get("skipped", None)
        output["offline"] = dct.get("offline", None)
        if dct["offline_timestamp"] == 0 | dct["offline_timestamp"] is None:
            output["offline_timestamp"] = None
        else:
            output["offline_timestamp"] = datetime.fromtimestamp(dct.get("offline_timestamp", 0) / 10**6, UTC)
        output["incognito_mode"] = dct.get("incognito_mode", None)

        return output
