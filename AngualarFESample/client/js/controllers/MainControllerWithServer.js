/**
 *
 */
var mapJSON={"India":{lat: 20.5937, lng: 78.9629}, "Srilanka":{lat: 7.8731, lng: 80.7718}, "Singapore":{lat: 1.3521, lng: 103.8198},"Australia":{lat:25.2744,lng:133.7751}};
var mapLatLng={};
app.controller('MainCtrl',function($scope,$http){
    $scope.countrylist = ["India", "Srilanka", "Singapore"];
    $scope.selection = {  };
    $scope.categories = [ { "name": "Sport", "id": "50d5ad" } , {"name": "General", "id": "678ffr" } ];
    $scope.selectedCountry = $scope.countrylist[0];
    $scope.setVolunteer = function(value) {
        $scope.selectedCountry = value;
    };

    $scope.$watch("selectedCountry",function(){
        mapLatLng=mapJSON[$scope.selectedCountry];
        initMap();
    });
    $scope.toggled = function(value) {
    };

    $scope.toggleDropdown = function($event) {
        $event.preventDefault();
        $event.stopPropagation();
        $scope.status.isopen = !$scope.status.isopen;
    };
    function getVolunteerList(){
        $http.get('/volunteerlist').success(function(response){
            $scope.volunteerList=response;
        });
    }

    getVolunteerList();
    $scope.volunteerList=[];
    $scope.addVolunteerInfo=function(){
        var skilsInfo={"clean":"Cleaning","cutting":"Vegetable Cutting","garnish":"Food Garnishing","serve":"Food Serving"}
        $scope.skilsList=[];
        $.each($scope.skils,function(key,value){
            $scope.skilsList.push(skilsInfo[key]);
        });
        $scope.volunteer.gender=$scope.gender;
        $scope.volunteer.country=$scope.selectedCountry;
        $scope.volunteer.skilsInfo=$scope.skilsList;
        $http.post('/addvolunteer',$scope.volunteer).success(function(response){
            $scope.success=true;
            console.log(JSON.stringify(response));
            $scope.volunteerList.push(response);
            $scope.volunteer="";
            getVolunteerList();
        });
    };

});

function initMap() {
    var map = new google.maps.Map(document.getElementById('map'), {
        zoom: 4,
        center: mapLatLng
    });
    var marker = new google.maps.Marker({
        position: mapLatLng,
        map: map
    });
}