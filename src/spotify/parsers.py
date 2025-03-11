from datetime import UTC, datetime


class SpotifyParser:
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

    def follow_data_parser(self, dct: dict) -> dict:
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

    def identifier_parser(self, dct: dict) -> dict:
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

    def marquee_parser(self, dct: dict) -> dict:
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

    def search_query_parser(self, dct: dict) -> dict:
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
        if len(dct["searchInteractionURIs"]) == 0:
            output["search_interaction_URIs"] = None
        else:
            output["search_interaction_URIs"] = dct.get("searchInteractionURIs", None)

        return output

    def user_data_parser(self, dct: dict) -> dict:
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
        output["birthdate"] = datetime.strptime(dct["birthdate"][:10], "%Y-%m-%d")  # noqa: DTZ007
        output["gender"] = dct["gender"]
        output["postal_code"] = dct.get("postalCode", None)
        output["mobile_number"] = dct.get("mobileNumber", None)
        output["mobile_operator"] = dct.get("mobileOperator", None)
        output["mobile_brand"] = dct.get("mobileBrand", None)
        output["creation_time"] = dct.get("creationTime", None)

        return output

    def _track_parser(self, dct: dict) -> dict:
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

    def _album_parser(self, dct: dict) -> dict:
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

    def _artist_parser(self, dct: dict) -> dict:
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

    def library_parser(self, dct: dict) -> dict:
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

    def streaming_history_parser(self, dct: dict) -> dict:
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
        if (dct["offline_timestamp"] == 0) | (dct["offline_timestamp"] is None):
            output["offline_timestamp"] = None
        else:
            output["offline_timestamp"] = datetime.fromtimestamp(dct.get("offline_timestamp", 0) / 10**6, UTC)
        output["incognito_mode"] = dct.get("incognito_mode", None)

        return output
