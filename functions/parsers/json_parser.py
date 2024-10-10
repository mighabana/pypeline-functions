from datetime import datetime, UTC


class GoogleTakeout:
    """
    """

    def chrome_history_parser(self, dct:dict) -> dict:
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
        """"""
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
        output = {}
        
        if "placeVisit" not in dct:
            return

        place_visit = dct['placeVisit']
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
        output["start_time"] = datetime.strptime(duration.startTimestamp, "%Y-%m-%dT%H:%M:%SZ") if duration.startTimestamp is not None else None
        output["end_time"] = datetime.strptime(duration.endTimestamp, "%Y-%m-%dT%H:%M:%SZ") if duration.endTimestamp is not None else None
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